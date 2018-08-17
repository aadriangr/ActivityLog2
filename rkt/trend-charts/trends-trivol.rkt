#lang racket/base

;; trends-trivol.rkt -- triathlon activity volume chart
;;
;; This file is part of ActivityLog2, an fitness activity tracker
;; Copyright (C) 2016 Alex Harsanyi (AlexHarsanyi@gmail.com)
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


(require
 racket/class
 racket/match
 racket/gui/base
 racket/math
 racket/format
 db/base
 plot/no-gui
 "../plot-hack.rkt"
 "../database.rkt"
 "../widgets/main.rkt"
 "trends-chart.rkt"
 "../sport-charms.rkt"
 "../fmt-util.rkt"
 "../plot-util.rkt")

(provide trivol-trends-chart%)

(define histogram-gap 0.5)

;; Group-by: 0 - week, 1 - month, 2 - year
;; Metric: 0 - time, 1 - distance, 2 - session count
(struct trivol-params tc-params (start-date end-date group-by metric))

(define trivol-chart-settings%
  (class edit-dialog-base%
    (init-field database
                [default-name "Trends"]
                [default-title "Trends Chart"])
    (super-new [title "Chart Settings"] [icon (edit-icon)] [min-height 10])

    (define name-gb (make-group-box-panel (send this get-client-pane)))
    (define name-field (new text-field% [parent name-gb] [label "Name "]))
    (send name-field set-value default-name)
    (define title-field (new text-field% [parent name-gb] [label "Title "]))
    (send title-field set-value default-title)

    (define time-gb (make-group-box-panel (send this get-client-pane)))
    (define date-range-selector (new date-range-selector% [parent time-gb]))

    (define grouping-gb (make-group-box-panel (send this get-client-pane)))
    (define group-by-choice
      (new choice% [parent grouping-gb] [label "Group By "]
           [choices '("Week" "Month" "Year")]))
    (define metric-choice
      (new choice% [parent grouping-gb] [label "Metric "]
           [choices '("Time" "Distance" "Session Count" "Effort")]))

    (define/public (get-restore-data)
      (list
       (send name-field get-value)
       (send title-field get-value)
       (send date-range-selector get-restore-data)
       (send group-by-choice get-selection)
       (send metric-choice get-selection)))

    (define/public (restore-from data)
      (when database
        (send date-range-selector set-seasons (db-get-seasons database)))
      (match-define (list d0 d1 d2 d3 d4) data)
      (send name-field set-value d0)
      (send title-field set-value d1)
      (send date-range-selector restore-from d2)
      (send group-by-choice set-selection d3)
      (send metric-choice set-selection d4))

    (define/public (show-dialog parent)
      (when database
        (send date-range-selector set-seasons (db-get-seasons database)))
      (if (send this do-edit parent)
          (get-settings)
          #f))

    (define/public (get-settings)
      (let ((dr (send date-range-selector get-selection)))
        (if dr
            (let ((start-date (car dr))
                  (end-date (cdr dr)))
              (when (eqv? start-date 0)
                (set! start-date (get-true-min-start-date database)))
              (trivol-params
               (send name-field get-value)
               (send title-field get-value)
               start-date
               end-date
               (send group-by-choice get-selection)
               (send metric-choice get-selection)))
            #f)))

    ))


(define (make-sql-query/time start-date end-date group-by)
  (format "select ~a as period,
           (total(T.strength_time) / 3600.0) as strength_time,
           (total(T.swim_time) / 3600.0) as swim_time,
           (total(T.bike_time) / 3600.0) as bike_time,
           (total(T.run_time) / 3600.0) as run_time
           from V_TRIATHLON_SESSIONS T
           where T.start_time between ~a and ~a group by period order by period"
          (cond ((eqv? group-by 0)       ; week
                 "date(T.start_time, 'unixepoch', 'localtime', '-6 days', 'weekday 1')")
                ((eqv? group-by 1)       ; month
                 "date(T.start_time, 'unixepoch', 'localtime', 'start of month')")
                ((eqv? group-by 2)       ; year
                 "date(T.start_time, 'unixepoch', 'localtime', 'start of year')")
                (#t
                 #f))
          start-date
          end-date))

(define (make-sql-query/count start-date end-date group-by)
  (format "select ~a as period,
           total(T.strength_count),
           total(T.swim_count),
           total(T.bike_count),
           total(T.run_count)
           from V_TRIATHLON_SESSIONS T
           where T.start_time between ~a and ~a group by period order by period"
          (cond ((eqv? group-by 0)       ; week
                 "date(T.start_time, 'unixepoch', 'localtime', '-6 days', 'weekday 1')")
                ((eqv? group-by 1)       ; month
                 "date(T.start_time, 'unixepoch', 'localtime', 'start of month')")
                ((eqv? group-by 2)       ; year
                 "date(T.start_time, 'unixepoch', 'localtime', 'start of year')")
                (#t
                 #f))
          start-date
          end-date))

(define (make-sql-query/distance start-date end-date group-by)
  (format "select ~a as period,
           0 as strength_distance,
           (total(T.swim_distance) / 1000.0),
           (total(T.bike_distance) / 1000.0),
           (total(T.run_distance) / 1000.0)
           from V_TRIATHLON_SESSIONS T
           where T.start_time between ~a and ~a group by period order by period"
          (cond ((eqv? group-by 0)       ; week
                 "date(T.start_time, 'unixepoch', 'localtime', '-6 days', 'weekday 1')")
                ((eqv? group-by 1)       ; month
                 "date(T.start_time, 'unixepoch', 'localtime', 'start of month')")
                ((eqv? group-by 2)       ; year
                 "date(T.start_time, 'unixepoch', 'localtime', 'start of year')")
                (#t
                 #f))
          start-date
          end-date))

(define (make-sql-query/stress start-date end-date group-by)
  (format "select ~a as period,
           total(T.strength_effort),
           total(T.swim_effort),
           total(T.bike_effort),
           total(T.run_effort)
           from V_TRIATHLON_SESSIONS T
           where T.start_time between ~a and ~a group by period order by period"
          (cond ((eqv? group-by 0)       ; week
                 "date(T.start_time, 'unixepoch', 'localtime', '-6 days', 'weekday 1')")
                ((eqv? group-by 1)       ; month
                 "date(T.start_time, 'unixepoch', 'localtime', 'start of month')")
                ((eqv? group-by 2)       ; year
                 "date(T.start_time, 'unixepoch', 'localtime', 'start of year')")
                (#t
                 #f))
          start-date
          end-date))

(define (get-data db sql-query)
  (query-rows db sql-query))

(define (generate-plot output-fn data y-label)
  (define max-y 0)
  (define pdata
    (for/list ([row data]
               [n (in-range (length data))])
      (if (> (vector-length row) 1)
          (let ((dummy #f))
            (match-define (vector timestamp wtime stime btime rtime) row)
            (set! max-y (max max-y (+ wtime stime btime rtime)))
            (list timestamp ;(if (eqv? (remainder n 12) 0) timestamp "")
                  (vector wtime stime btime rtime)))
          (list "" (vector)))))
  (set! max-y (* 1.2 max-y)) ;; make it larger to fit the legend
  (parameterize ([plot-x-ticks (date-ticks)]
                 [plot-x-label #f]
                 [plot-x-tick-label-anchor 'top-right]
                 [plot-x-tick-label-angle 30]
                 [plot-y-label y-label])
    (output-fn
     (list (y-tick-lines)
           (stacked-histogram
            pdata
            #:y-max max-y
            #:colors
            (list (get-sport-color 4 20)
                  (get-sport-color 5 #f)
                  (get-sport-color 2 #f)
                  (get-sport-color 1 #f))
            #:labels '("Weights" "Swim" "Bike" "Run")
            #:line-widths '(0 0 0 0)
            #:gap histogram-gap))
     0 (length pdata) 0 max-y)))

(define (insert-plot-snip canvas data y-label)
  (generate-plot
   (lambda (renderer-tree min-x max-x min-y max-y)
     (plot-snip/hack canvas
                     #:x-min min-x
                     #:x-max max-x
                     #:y-min min-y
                     #:y-max max-y
                     renderer-tree))
   data y-label))

(define (save-plot-to-file file-name width height data y-label)
  (generate-plot
   (lambda (renderer-tree min-x max-x min-y max-y)
     (plot-file renderer-tree file-name #:width width #:height height
                #:x-min min-x
                #:x-max max-x
                #:y-min min-y
                #:y-max max-y))
   data y-label))

(define trivol-trends-chart%
  (class trends-chart% (init-field database) (super-new)

    (define data-valid? #f)
    (define sql-query #f)
    (define sql-query-result #f)
    (define chart-data #f)

    (define/override (make-settings-dialog)
      (new trivol-chart-settings%
           [default-name "TriVol"]
           [default-title "Multisport Training Volume"]
           [database database]))

    (define/override (invalidate-data)
      (set! data-valid? #f)
      (set! cached-hslot #f)
      (set! cached-badge #f))

    (define/override (is-invalidated-by-events? events)
      (or (hash-ref events 'session-deleted #f)
          (hash-ref events 'session-created #f)
          (hash-ref events 'session-updated #f)))

    (define (get-y-label)
      (let ((metric (trivol-params-metric (send this get-params))))
        (case metric
          ((0) "Time (hours)")
          ((1) "Distance (km)")
          ((2) "Session Count")
          ((3) "Effort"))))

    (define cached-hslot #f)
    (define cached-badge #f)

    (define (plot-hover-callback snip event x y)
      (define renderers '())
      (define (add-renderer r) (set! renderers (cons r renderers)))
      (define skip (discrete-histogram-skip))
      (define gap histogram-gap)
      
      (when (good-hover? x y event)
        (define params (send this get-params))
        (define metric (trivol-params-metric params))
        (define format-value
          (case metric
            ((0) (lambda (v) (duration->string (* v 3600))))
            ((1) (lambda (v) (distance->string (* v 1000) #t)))
            ((2) (lambda (v) (format "~a activities" (exact-round v))))
            ((3) (lambda (v) (format "~a stress" (exact-round v))))))
        
        (define-values (series slot) (xposition->histogram-slot x skip gap))
        (when (and chart-data series slot (= series 0) (< slot (length chart-data)))
          (let ((row (list-ref chart-data slot)))
            (when (> (vector-length row) 1)
              (match-define (vector timestamp wtime stime btime rtime) row)
              (define total (+ wtime stime btime rtime))
              (define (->percent-str val)
                (string-append (~r (* 100 (/ val total)) #:precision 1) " %"))
              (when (<= y total)
                (unless (eq? cached-hslot slot)
                  (set! cached-hslot slot)
                  (set! cached-badge
                        (make-hover-badge
                         (list (list "Weights" (format-value wtime) (->percent-str wtime))
                               (list "Swim" (format-value stime) (->percent-str stime))
                               (list "Bike" (format-value btime) (->percent-str btime))
                               (list "Run" (format-value rtime) (->percent-str rtime))
                               (list "Total" (format-value (+ wtime stime btime rtime)))))))
                (when cached-badge
                  (add-renderer (pu-label x y cached-badge))))))))
      (set-overlay-renderers snip renderers))

    (define/override (put-plot-snip canvas)
      (maybe-fetch-data)
      (if data-valid?
          (let ((snip (insert-plot-snip canvas chart-data (get-y-label))))
            (set-mouse-event-callback snip plot-hover-callback))
          (begin
            (send canvas set-snip #f)
            (send canvas set-background-message "No data to plot"))))

    (define/override (save-plot-image file-name width height)
      ;; We assume the data is ready, and don't do anything if it is not.
      (when data-valid?
          (save-plot-to-file file-name width height chart-data (get-y-label))))

    (define/override (export-data-to-file file formatted?)
      (when chart-data
        (call-with-output-file file
          (lambda (out) (export-data-as-csv out formatted?))
          #:mode 'text #:exists 'truncate)))

    (define (export-data-as-csv out formatted?)
      (write-string "Timestamp, Strength, Swim, Bike, Run" out)
      (newline out)

      (for ((datum chart-data) #:when (> (vector-length datum) 1))
        (match-define (vector timestamp wtime stime btime rtime) datum)        
        (write-string
         (format "~a, ~a, ~a, ~a, ~a~%" timestamp wtime stime btime rtime))
         out))

    (define (maybe-fetch-data)
      (unless data-valid?
        (let ((params (send this get-params)))
          (when params
            (let* ((start (trivol-params-start-date params))
                   (end (trivol-params-end-date params))
                   (group-by (trivol-params-group-by params))
                   (metric (trivol-params-metric params))
                   (timestamps (generate-timestamps start end group-by)))
              (set! sql-query
                    (case metric
                      ((0) (make-sql-query/time start end group-by))
                      ((1) (make-sql-query/distance start end group-by))
                      ((2) (make-sql-query/count start end group-by))
                      ((3) (make-sql-query/stress start end group-by))))
              (set! sql-query-result (get-data database sql-query))
              (when (> (length sql-query-result) 0)
                (set! chart-data (reverse (pad-data timestamps sql-query-result)))
                (set! chart-data (simplify-labels chart-data group-by))
                (set! data-valid? #t)))))))

    ))
