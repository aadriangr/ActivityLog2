#lang racket/base

;; xdata.rkt --
;;
;; This file is part of ActivityLog2 -- https://github.com/alex-hhh/ActivityLog2
;; Copyright (c) 2018 Alex Harsányi <AlexHarsanyi@gmail.com>
;;
;; This program is free software: you can redistribute it and/or modify it
;; under the terms of the GNU General Public License as published by the Free
;; Software Foundation, either version 3 of the License, or (at your option)
;; any later version.
;;
;; This program is distributed in the hope that it will be useful, but WITHOUT
;; ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
;; FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
;; more details.
;;
;; You should have received a copy of the GNU General Public License along
;; with this program.  If not, see <http://www.gnu.org/licenses/>.

(require
 db/base
 racket/match
 racket/class
 racket/contract
 racket/async-channel
 "series-metadata.rkt"
 "native-series.rkt"
 "../utilities.rkt"
 "../data-frame/df.rkt"
 "../data-frame/series.rkt")

(provide/contract
 (read-xdata-series (-> data-frame? connection? any/c)))

(define sql-query
  "select coalesce(XF.native_message, -1) as native_message,
       coalesce(XF.native_field, -1) as native_field,
       coalesce(XF.headline, XF.name, 'Unnamed') as headline,
       coalesce(XF.axis_label, XF.unit_name, 'Unlabeled') as axis_label,
       coalesce(XF.series_name, 'xdata-' || XF.id) as sn,
       XF.fractional_digits as fd,
       XF.missing_value as mv,
       XF.histogram_bucket_slot as hbs,
       XF.inverted_mean_max as imm,
       XF.should_filter as f,
       XF.color as c
  from XDATA_FIELD XF
 where XF.id = ?")

;; Construct a series metadata object from an XDATA field.  This object
;; provides information such as what headline and axis label to use for the
;; plots as well as defaults for some operations
(define (make-xdata-series-metadata db field-id)
  (define row (query-maybe-row db sql-query field-id))
  (unless row
    (raise-argument-error 'field-id "valid XDATA_FIELD.id" field-id))
  (match-define (vector nm nf hl label series fd mv hbs imm f c) row)
  ;; Construct a new metadata class.  The parent of our metadata class is
  ;; `series-metadata%` by default, but we also look at the native field value
  ;; and choose a better parent in some cases -- currently we choose
  ;; `power-series-metadata%` for "power" native fields -- this ensures that
  ;; power series collected as XDATA can have CP estimation...
  (define xdata-series-metadata%
    (class (if (eqv? nm 20)             ; a record message
               (case nf
                 ((7) power-series-metadata%)
                 (else series-metadata%))
               ;; Use generic base class for non native messages
               series-metadata%)
      (init) (super-new)
      (define/override (should-filter?)
        (if (sql-null? f) (super should-filter?) f))
      (define/override (histogram-bucket-slot)
        (if (sql-null? hbs) (super histogram-bucket-slot) hbs))
      (define/override (inverted-mean-max?)
        (if (sql-null? imm) (super inverted-mean-max?) imm))
      (define/override (axis-label)
        (if (sql-null? label) (super axis-label) label))
      (define/override (headline)
        (if (sql-null? hl) (super headline) hl))
      (define/override (series-name) series) ; cannot be NULL! as this needs to be unique
      (define/override (fractional-digits)
        (if (sql-null? fd) (super fractional-digits) fd))
      (define/override (missing-value)
        (if (sql-null? mv) (super missing-value) mv))))
  (new xdata-series-metadata%))

(define the-xdata-registry (make-hash))
(define the-log-event-source (make-log-event-source))

(define (get-or-make-xdata-series-metadata db field-id)
  (define metadata (hash-ref the-xdata-registry field-id #f))
  (unless metadata
    (set! metadata (make-xdata-series-metadata db field-id))
    ;; Register this for both lap swim and normal activities
    (register-series-metadata metadata #f)
    (register-series-metadata metadata #t)
    (hash-set! the-xdata-registry field-id metadata))
  metadata)

(define xdata-values-query
  "select T.timestamp, XV.field_id, XV.val
  from XDATA_VALUE XV,
       A_TRACKPOINT T,
       A_LENGTH L,
       A_LAP P
 where XV.trackpoint_id = T.id
   and T.length_id = L.id
   and L.lap_id = P.id
  and P.session_id = ?
order by T.timestamp")

;; When a new database is opened, remove the XDATA series that were added in
;; the previous one.  This needs to be called from `read-xdata-series` and
;; will do the right thing when the first session from the new database is
;; read.
(define (maybe-flush-xdata-series)
  (let loop ((item (async-channel-try-get the-log-event-source)))
    (when item
      (match-define (list tag data) item)
      ;; unregister our series metadata when the database changes -- new ones
      ;; will be created when session data is read in.
      (when (eq? tag 'database-opened)
        (for (([key value] (in-hash the-xdata-registry)))
          ;; Unregister form both lap swim and other activities.
          (unregister-series-metadata value #t)
          (unregister-series-metadata value #f))
        (set! the-xdata-registry (make-hash)))
      (loop (async-channel-try-get the-log-event-source)))))

;; Read all (or any) XDATA series for the session in the data frame `df` and
;; also create any xdata series metadata objects required for the series in
;; this data frame.
(define (read-xdata-series df db)

  (maybe-flush-xdata-series)

  (define sid (df-get-property df 'session-id))
  (define current-ts #f)
  (define position #f)
  (define xdata-series (make-hash))

  (for (([ts field value] (in-query db xdata-values-query sid)))
    (unless (equal? current-ts ts)
      (set! current-ts ts)
      (set! position (df-index-of df "timestamp" ts)))
    (define xdata (hash-ref xdata-series field #f))
    (unless xdata
      (set! xdata (make-vector (df-row-count df) #f))
      (hash-set! xdata-series field xdata))
    (vector-set! xdata position value))

  (for ([(key value) (in-hash xdata-series)])
    (define metadata (get-or-make-xdata-series-metadata db key))
    (define series (make-series (send metadata series-name) #:data value))
    (df-add-series df series)))
