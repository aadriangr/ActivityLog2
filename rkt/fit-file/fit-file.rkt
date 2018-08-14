#lang racket/base
;; fit-file.rkt -- read and write .FIT files.

;; This file is part of ActivityLog2, an fitness activity tracker
;; Copyright (C) 2015, 2018 Alex Harsányi <AlexHarsanyi@gmail.com>
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

;;; Commentary:
;;
;; ACTIVITY .FIT files can be read, WORKOUT, SPORT and SETTINGS FIT files can
;; be written.
;;
;; The structure of a FIT file is described in the FIT SDK which you can
;; download form https://www.thisisant.com/resources/fit/
;;
;; DEVELOPER FIELDS SUPPORT (FIT 2.0)
;;
;; This code supports reading FIT files with developer fields -- these are
;; recorded by 3rd party devices, such as running power and blood oxygen
;; monitors.  Activities containing such fields can be read with the following
;; limitations:
;;
;; An activity will record 3rd party applications in the 'developer-data-id
;; entry, which contains a list defining the application identifier (16 byte
;; value) to an application index.  It looks like this:
;;
;; (developer-data-ids
;;    ((start-time . 1530870414)
;;     (developer-id . "ffffffffffffffffffffffffffffffff")
;;     (application-id . "27dfb7e5900f4c2d80abc57015f42124")
;;     (application-version . 9)
;;     (developer-data-index . 0)))
;;
;; Fields are defined in the 'field-descriptions entry.  For each field, we
;; have the application index, the field number, the native field number (if
;; this field corresponds to a native field in the record), the type, a name
;; and a unit.  In addition, there is a field-key entry (added by this code)
;; which will be used to identify the fields in the various data records
;; (sessions, laps, lengths or track points).  It looks like this:
;;
;; (field-descriptions
;;    ((start-time . 1530870414)
;;     (field-name . "eE")
;;     (field-key . 27dfb7e5900f4c2d80abc57015f42124-1)
;;     (units . #"c/hr")
;;     (native-msg-num . 20)
;;     (developer-data-index . 0)
;;     (field-def-number . 1)
;;     (fit-base-type . 132))
;;    ((start-time . 1530870414)
;;     (field-name . "StrideDistance")
;;     (field-key . 27dfb7e5900f4c2d80abc57015f42124-0)
;;     (units . #"ft or m")
;;     (native-msg-num . 20)
;;     (developer-data-index . 0)
;;     (field-def-number . 0)
;;     (fit-base-type . 136)))
;;
;; From the example above, trackpoints will contain a key of
;; 27dfb7e5900f4c2d80abc57015f42124-0 for the stride distance value of that
;; point.

(require racket/class
         racket/file
         racket/list
         racket/match
         racket/math
         racket/port
         racket/dict
         racket/format
         "activity-util.rkt"
         "fit-defs.rkt"
         "../utilities.rkt")

(provide make-fit-data-stream)
(provide read-fit-records)
(provide fit-event-dispatcher%)
(provide activity-builder%)
(provide read-activity-from-file)
(provide fit-get-device-name)
(provide fit-output-stream%)
(provide fit-output-file%)
(provide mk-fit-string)
(provide fit-workout-file%)
(provide fit-sport-file%)
(provide fit-settings-file%)



;............................................................... basics ....

(define (raise-error msg . args)
  (apply error 'fit-file msg args))

(define (compute-crc buffer [start 0] [end #f])
  ;; Compute the CRC of the bytes in BUFFER between START and END and return
  ;; it.  The CRC algorithm is the same as described int the FIT file
  ;; documentation.  When verifying CRC, the assumption is that the CRC are
  ;; the last two bytes in the buffer and a return value of 0 indicates that
  ;; the file's CRC is OK.

  (define crc-table #(#x0000 #xCC01 #xD801 #x1400
                      #xF001 #x3C00 #x2800 #xE401
                      #xA001 #x6C00 #x7800 #xB401
                      #x5000 #x9C01 #x8801 #x4400))

  (define limit (or end (bytes-length buffer)))

  (define (update-nibble crc b)
    (let ((tmp (vector-ref crc-table (bitwise-and crc #xf))))
      (let ((crc (bitwise-and (arithmetic-shift crc -4) #x0FFFF)))
        (bitwise-xor (bitwise-xor crc tmp)
                     (vector-ref crc-table (bitwise-and b #xf))))))

  (define (update-byte crc b)
    (update-nibble (update-nibble crc (bitwise-and b #xf))
                   (bitwise-and (arithmetic-shift b -4) #xf)))

  (let loop ((idx start)
             (crc 0))
    (if (= idx limit)
        crc
        (loop (+ idx 1) (update-byte crc (bytes-ref buffer idx))))))


;;...................................................... fit basic types ....

(define (real->bstr val size signed? big-endian? dest-bstr start)
  ;; Write VAL, a floating point value to DEST-BSTR.  This is just a wrapper
  ;; around `real->floating-point-bytes' to add an unused SIGNED? parameter so
  ;; the signature is consistent with what we need from a FIT read function.
  (real->floating-point-bytes val size big-endian? dest-bstr start))

(define (bstr->real bstr signed? big-endian? start end)
  ;; Read a floating point value from a byte string.  This is just a wrapper
  ;; around `floating-point-bytes->real' to add an unused SIGNED? parameter so
  ;; the signature is consistend with what we need from a FIT read function.
  (floating-point-bytes->real bstr big-endian? start end))

(define (integer->bstr val size signed? big-endian? dest-bstr pos)
  ;; Write an integer to a byte string. This is a wrapper for
  ;; `integer->integer-bytes' except that function does not handle 1 byte
  ;; integers.
  (if (> size 1)
      (integer->integer-bytes val size signed? big-endian? dest-bstr pos)
      (bytes-set! dest-bstr pos val)))

(define (bstr->integer bstr signed? big-endian? start end)
  ;; Read an integer from a byte string.  This is a wrapper for
  ;; `integer-bytes->integer' except that function does not handle 1 byte
  ;; integers, so we need to handle it ourselves.
  (if (> (- end start) 1)
      (integer-bytes->integer bstr signed? big-endian? start end)
      ;; we need to handle a size of 1 separately, as integer-bytes->integer
      ;; handles a min 2 byte integer...
      (let ((b (bytes-ref bstr start)))
        (if signed?
            (if (= (bitwise-and b #x80) #x80)
                (- b #x100)
                b)
            b))))

(struct fit-type
  ;; Hold information about a FIT basic type
  (id                                   ; a number as defined in the FIT documentation
   name                                 ; a convenient name for it
   signed?                              ; is this type signed?
   size                                 ; how many bytes does it need?
   ;; a special value whose meaning is "invalid".
   invalid-value
   ;; a function that reads a value this type from a byte string
   read-fn
   ;; a function that writes a value of this type to a byte string
   write-fn)
  #:transparent)

(define fit-types
  ;; Definition of all basic FIT types, as per the FIT file documentation.
  ;; This is a hash with the type ID being the key, see also `get-fit-type'
  (hash
   #x00 (fit-type #x00 'enum #f 1 #xFF bstr->integer integer->bstr)

   #x01 (fit-type #x01 'sint8 #t 1 #x7F bstr->integer integer->bstr)
   #x02 (fit-type #x02 'uint8 #f 1 #xFF bstr->integer integer->bstr)

   #x83 (fit-type #x83 'sint16 #t 2 #x7FFF bstr->integer integer->bstr)
   #x84 (fit-type #x84 'uint16 #f 2 #xFFFF bstr->integer integer->bstr)

   #x85 (fit-type #x85 'sint32 #t 4 #x7FFFFFFF bstr->integer integer->bstr)
   #x86 (fit-type #x86 'uint32 #f 4 #xFFFFFFFF bstr->integer integer->bstr)

   #x07 (fit-type #x07 'string #f 1 #x00 bstr->integer integer->bstr)

   #x88 (fit-type #x88 'float32 #f 4 #xFFFFFFFF bstr->real real->bstr)
   #x89 (fit-type #x89 'float64 #f 8 #xFFFFFFFFFFFFFFFF bstr->real real->bstr)

   #x0a (fit-type #x0a 'uint8z #f 1 #x00 bstr->integer integer->bstr)
   #x8b (fit-type #x8b 'uint16z #f 2 #x00 bstr->integer integer->bstr)
   #x8c (fit-type #x8c 'uint32z #f 4 #x00 bstr->integer integer->bstr)

   #x0d (fit-type #x0d 'byte #f 1 #xFF bstr->integer integer->bstr)))

(define (get-fit-type id)
  ;; Return a fit type based on the ID which can be the actual fit type, its
  ;; number or name.  Return #f if the type is not found or ID is not one of
  ;; the expected values.
  (cond ((fit-type? id) id)
        ((number? id) (hash-ref fit-types id))
        ((symbol? id)
         (for/first ([t (in-hash-values fit-types)]
                     #:when (eq? id (fit-type-name t)))
           t))
        (#t #f)))

(define (read-one-fit-value buf pos type big-endian?)
  ;; Read a single value from BUF@POS of the specified TYPE.
  (let ((read-fn (fit-type-read-fn type))
        (size (fit-type-size type))
        (signed? (fit-type-signed? type)))
    (when (> (+ pos size) (bytes-length buf))
      (raise-error "read past end of buffer"))
    (let ((raw-val (bstr->integer buf #f big-endian? pos (+ pos size))))
      (if (equal? raw-val (fit-type-invalid-value type))
          (values #f (+ pos size))
          (values
           (read-fn buf signed? big-endian? pos (+ pos size))
           (+ pos size))))))

(define (read-fit-value buf pos size type big-endian?)
  ;; Read one or more values from BUF@POS of the specified type.  SIZE is the
  ;; total size of the bytes to read (SIZE / SIZEOF(type) determines the
  ;; number of values read).  Returns two values: value or vector of values
  ;; plus the new buffer position.
  (let ((nitems (/ size (fit-type-size type))))
    (if (equal? nitems 1)
        (read-one-fit-value buf pos type big-endian?)
        (let ((result (make-vector nitems #f)))
          (let loop ((pos pos)
                     (i 0))
            (when (< i nitems)
              (let-values (([v p] (read-one-fit-value buf pos type big-endian?)))
                (vector-set! result i v)
                (loop p (+ i 1)))))
          (values result (+ pos size))))))

(define (write-one-fit-value buf pos type value big-endian?)
  ;; Write at BUF@POS the specified VALUE according to TYPE.  If VALUE is #f,
  ;; the type's "invalid" value is written (see the fit-type struct
  ;; definition).  Returns the new buffer position.
  (let ((write-fn (fit-type-write-fn type))
        (size (fit-type-size type))
        (signed? (fit-type-signed? type)))
    (when (> (+ pos size) (bytes-length buf))
      (raise-error "write past end of buffer"))
    ;; NOTE: if value is #f, we write the invalid value to the stream.
    (write-fn (or value (fit-type-invalid-value type))
              size signed? big-endian? buf pos)
    (+ pos size)))

(define (write-fit-value buf pos type value big-endian?)
  ;; Write at BUF@POS the specified VALUE accorting to TYPE.  VALUE can be
  ;; either #f, an individual value or a vector of values.
  (cond ((vector? value)
         (let loop ((index 0)
                    (pos pos))
           (if (< index (vector-length value))
               (loop
                (+ index 1)
                (write-one-fit-value buf pos type (vector-ref value index) big-endian?))
               pos)))
        (#t
         (write-one-fit-value buf pos type value big-endian?))))


;..................................................... fit-data-stream% ....

(define fit-data-stream%
  ;; Helper class to read values from a FIT file.  The object is initialized
  ;; with a byte string, it will validate the header than provide a
  ;; `read-next-value' method to read values.  It will keep track of the
  ;; current buffer position internally.

  (class object% (init data) (super-new)

    (define buffer data)
    (define crtpos 0)
    (define limit (bytes-length buffer)) ; will be updated below

    (define/public (is-eof?) (>= crtpos limit))

    (define/public (position) crtpos)

    (define/public (read-next-value type-id [size #f] [big-endian? #f])
      ;; Read a value of the specified type (TYPE-ID) from the stream at the
      ;; current position (which will be updated).
      (let* ((type (get-fit-type type-id))
             (sz (or size (fit-type-size type))))
        (let-values (([result new-crtpos]
                      (read-fit-value buffer crtpos sz type big-endian?)))
          (set! crtpos new-crtpos)
          (if (and result (eq? (fit-type-name type) 'string))
              ;; convert the result to a string
              (if (equal? size 1)
                  (bytes result)
                  (let ((b '()))
                    (let loop ((idx 0))
                      (when (and (< idx (vector-length result)) (vector-ref result idx))
                        (set! b (cons (vector-ref result idx) b))
                        (loop (+ 1 idx))))
                    (apply bytes (reverse b))))
              result))))

    ;; Start reading the header and performing validation on the FIT
    ;; file. WARNING: we depend on these declaration being in the order below,
    ;; as we are just reading from the buffer now.

    (define header-length (read-next-value 'uint8))

    (unless (>= header-length 12) (raise-error "bad header length"))

    (define protocol-version (read-next-value 'uint8))
    (define profile-version (read-next-value 'uint16))
    (define data-length (read-next-value 'uint32))
    (unless (equal? (read-next-value 'string 4) #".FIT")
      (raise-error "missing .FIT signature"))

    ;; two bytes count the CRC at the end.
    (let ((expected (+ header-length data-length 2))
          (actual (bytes-length buffer)))
      ;; NOTE: fit files can contain chunks, which are effectively multiple
      ;; FIT files concatenated, so we check for actual being greater or equal
      ;; to expected. This FIT file feature is used to record HR data in
      ;; swimming activities (where the strap stores the data and transmits it
      ;; at the end of the activity).  We don't support loading HR data from
      ;; such FIT files yet, but at least we can load the swimming part.
      (unless (>= actual expected)
        (raise-error "bad data-length: ~a, expecting ~a" actual expected)))

    (unless (= (compute-crc buffer) 0)
      (raise-error "bad file CRC"))

    ;; Now that we have read the header, update crtpos and limit to match the
    ;; actual data content of the buffer.
    (set! crtpos header-length)
    (set! limit (+ header-length data-length))

    ))

(define (make-fit-data-stream source)
  ;; Construct a fit-data-stream% from SOURCE which can be either a byte
  ;; string, a file name or port.
  (let ([data (cond ((bytes? source) source)
                    ((or (path? source) (string? source))
                     (file->bytes source #:mode 'binary))
                    ((input-port? source)
                     (port->bytes source)))])
    (new fit-data-stream% [data data])))


;.................................... reading records from a fit stream ....

(define (decode-record-header header)
  ;; Decode the header for a record in a fit file.  The header is a single
  ;; byte.  We return a list of:
  ;; * header type ('normal or 'compressed-timestamp)
  ;; * 'data or 'defintion record
  ;; * local-message-id
  ;; * maybe timestamp offset for a compressed-timestamp header.
  (let ((header-type (if (= (bitwise-and header #x80) 0)
                         'normal 'compressed-timestamp)))
    (if (eq? header-type 'normal)
        (list
         'normal
         (if (= (bitwise-and header #x40) 0) 'data 'definition)
         (bitwise-and header #x0F)      ; local message id
         (if (= (bitwise-and header #x20) 0) 'standard 'custom)
         )
        (list
         'compressed-timestamp
         'data
         (bitwise-bit-field header 5 7) ; local message id, note that the bit field is open ended! ()
         (bitwise-and header #x1F)      ; timestamp
         ))))

(define (read-message-definition fit-stream standard-or-custom)
  ;; Read a message definition from the FIT-STREAM.  A message definition will
  ;; tell us what fields are available in a certain message and what the type
  ;; of these fields is.  STANDARD-OR-CUSTOM is a symbol (either 'standard or
  ;; 'custom) which tells us if we should expect developer fields in the
  ;; message definition.
  (send fit-stream read-next-value 'uint8) ; reserved field, skip it
  (let* ((arhitecture (if (= (send fit-stream read-next-value 'uint8) 0)
                          'little-endian 'big-endian))
         (global-message-number (send fit-stream read-next-value 'uint16 2
                                      (eq? arhitecture 'big-endian)))
         (global-message-name (dict-ref *global-message-number* global-message-number #f))
         (field-count (send fit-stream read-next-value 'uint8))
         (field-names (if global-message-name
                          (dict-ref *field-db* global-message-name #f)
                          #f)))
    (append
     (list arhitecture (or global-message-name global-message-number))
     ;; Standard fields come first
     (for/list ([i (in-range field-count)])
       (let* ((number (send fit-stream read-next-value 'uint8))
              (size (send fit-stream read-next-value 'uint8))
              (type (send fit-stream read-next-value 'uint8))
              (name (if field-names (dict-ref field-names number #f) #f)))
         (list (or name number) size type)))
     ;; Developer specific fields (if any) come last
     (let ((dev-field-count (if (eq? standard-or-custom 'custom)
                                (send fit-stream read-next-value 'uint8)
                                0)))
       (for/list ([i (in-range dev-field-count)])
         (let* ((number (send fit-stream read-next-value 'uint8))
                (size (send fit-stream read-next-value 'uint8))
                (ddi (send fit-stream read-next-value 'uint8))) ; dev data index
           ;; Dev data fields are encoded by adding 1000 to them, so they are
           ;; not confused with FIT types, which are all less than 255.
           (list number size (+ 1000 ddi))))))))

(define (make-message-reader definition dev-field-types)
  ;; Return a function which will read a message from a FIT-STREAM according
  ;; to DEFINITION (as constructed by `read-message-definition')
  ;; DEV-FIELD-TYPES contains a mapping from a DDI to the actual FIT type for
  ;; the field.

  (define (convert-value value field-name conversions)
    ;; Convert VALUE for FIELD-NAME into a more usable format accorting to the
    ;; CONVERSIONS ALIST.  For example, speed is stored multiplied by 1000
    ;; (that is, millimeters per second) so we convert it back to
    ;; meters/second.  Latitude and Longitude are stored as "semirircles", we
    ;; convert them back to degrees.
    (let ((convert-fn (cond ((assq field-name conversions) => cdr)
                            (#t (lambda (x) x)))))
      (if (vector? value)
          (for/vector ((v (in-vector value))) (convert-fn v))
          (convert-fn value))))

  (define conversion-table
    ;; conversion-table to use with `convert-value' for this message id.
    (cond ((assq (second definition) *field-conversion-db*) => cdr)
          (#t '())))

  (define (read-value-fn type size stream)
    (send stream read-next-value type size big-endian?))

  (define big-endian?
    (not (eq? (car definition) 'little-endian)))

  (lambda (stream)
    (for/list ([field (cdr (cdr definition))])
      (match-define (list name size type) field)
      (cond ((>= type 1000)
             ;; this is a DDI, find the actual type and read it.  Don't do any
             ;; conversion on i, but use the specified field name for it (if
             ;; it is available)
             (match-let (((list dname dtype)
                          (hash-ref dev-field-types
                                    (cons type name)
                                    (lambda () (raise-error "unknown dev field: ~a" (- type 1000))))))
               (cons (or dname name) (read-value-fn dtype size stream))))
            (#t
             (let ((value (read-value-fn type size stream)))
               (cons name (and value (convert-value value name conversion-table)))))))))

(define (read-fit-records fit-stream dispatcher)
  ;; Read all data records from FIT-STREAM (a fit-data-stream%) and send them
  ;; to the DISPATCHER by calling its dispatch method.  This function keeps
  ;; track of message defintions and uses them to decode the actual messages.
  ;; The DISPATCHER is an object with a signle method "dispatch message-num
  ;; data", but see fit-event-dispatcher% for a nicer interface.

  (define message-readers (make-hash))
  ;; Map a dev-data index to the basic FIT field type for that field.
  (define dev-field-types (make-hash))
  (define app-defs (make-hash))

  (define (dev-field-key ddi number name)
    (let ((app-id (hash-ref app-defs ddi #f)))
      (if app-id
          (string->symbol (string-append app-id "-" (~a number)))
          (string->symbol (bytes->string/utf-8 name)))))

  (define (make-string-id id)
    (apply string-append
           (for/list ([i (in-vector id)])
             (if (number? i)
                 (~r i #:precision 0 #:base 16 #:min-width 2 #:pad-string "0")
                 "ff"))))

  (define (read-next-record)
    (let* ((hdr (or (send fit-stream read-next-value 'uint8) 255))
           (header (decode-record-header hdr)))
      ;; (printf "header(~a pos = ~a): ~a~%" hdr (send fit-stream position) header)
      (match-define (list htype def-or-data local-id rest ...) header)
      (cond ((eq? def-or-data 'definition)
             (let ((def (read-message-definition fit-stream (car rest))))
               ;; (display def)(newline)
               ;; (display (format "DEFN local: ~a, global: ~a, ~a field(s)~%"
               ;;                  (third header)
               ;;                  (second def)
               ;;                  (length (cdr (cdr def)))))
               (hash-set! message-readers
                          local-id
                          (cons (second def) (make-message-reader def dev-field-types))))
             #t)
            ((eq? def-or-data 'data)
             (let ((reader (hash-ref message-readers local-id #f)))
               (unless reader
                 (raise-error "no reader for local message id ~a" header))
               ;; (display (format "DATA local: ~a (~a)~%" (third header) (car reader)))
               (let ((message-id (car reader))
                     (message-data ((cdr reader) fit-stream)))
                 ;; (printf "DATA CONTENTS: ~a~%" message-data)
                 (cond ((eq? message-id 'developer-data-id)
                        ;; A developer-data-id message "announces" a new XDATA
                        ;; application.  We convert the developer-id and
                        ;; application-id fields from an array of bytes to
                        ;; string guid and also record this application in
                        ;; APP-DEFS
                        (let ((devid (dict-ref message-data 'developer-id #f))
                              (appid (dict-ref message-data 'application-id #f))
                              (ddi (dict-ref message-data 'developer-data-index #f)))
                          (when appid
                            (define app-key (make-string-id appid))
                            (set! message-data
                                  (cons (cons 'application-id app-key)
                                        (dict-remove message-data 'application-id)))
                            (when ddi
                              (hash-set! app-defs ddi app-key)))
                          (when devid
                            (set! message-data
                                  (cons (cons 'developer-id (make-string-id devid))
                                        (dict-remove message-data 'developer-id))))))
                       ((eq? message-id 'field-description)
                        ;; A field-description message "announces" a new XDATA
                        ;; field.  The field will be present in the records
                        ;; using a unique key (see below). The key is also
                        ;; added to the field as a 'field-key entry for easier
                        ;; processing of XDATA later on.
                        (let* ((ddi (dict-ref message-data 'developer-data-index #f))
                               (type (dict-ref message-data 'fit-base-type #f))
                               (number (dict-ref message-data 'field-def-number #f))
                               (name (dict-ref message-data 'field-name #f))
                               (key (dev-field-key ddi number name)))
                          (hash-set! dev-field-types (cons (+ 1000 ddi) number) (list key type))
                          (set! message-data (cons (cons 'field-key key) message-data))
                          (when name
                            (set! message-data
                                  (cons (cons 'field-name (bytes->string/utf-8 name))
                                        (dict-remove message-data 'field-name)))))))
                 ;; Developer data ID and field description messages are also
                 ;; sent to the dispatcher, which will be responsibe for
                 ;; interpreting these fields.  The decoder will use the
                 ;; field-key, not the field ID.
                 (send dispatcher dispatch
                       message-id
                       (if (eq? htype 'compressed-timestamp)
                           (cons (cons 'compressed-timestamp (car rest)) message-data)
                           message-data)))))
            (#t
             (raise-error "bad header: ~a" header)))))

  (define (loop)
    (unless (send fit-stream is-eof?)
      (read-next-record)
      (loop)))

  (loop))


;................................................ fit-event-dispatcher% ....

(define fit-event-dispatcher%
  ;; Decode and dispatch fit-event messsages to different "on-..." methods
  ;; that can be overriden by the user to do something usefull.  Also keeps
  ;; track of the current time in the FIT message and expands
  ;; 'compresset-timestamp' fields.  An object derived from this class can be
  ;; passed to `read-fit-records'
  ;;
  ;; During FIT file parsing, `read-fit-records` will pass the decoded FIT
  ;; file messages to a builder, an object derived from fit-event-dispatcher%
  ;; using various "on-" methods, one for each message type.  The builder
  ;; object will collect the messages and construct a Racket structure that is
  ;; suitable for the application.  Understanding the various messages
  ;; requires familiarity with the FIT file format and its messages.  These
  ;; are available in the FitSDK, in the Profile Excel document in it.

  (class object% (init) (super-new)

    (define start-timestamp #f)
    (define current-timestamp #f)

    (define (update-timestamp record)

      ;; Update start-timestamp, current-timestamp from the current RECORD,
      ;; taking special care not to move the time backwards.
      (let ((ts (dict-ref record 'timestamp current-timestamp)))
        (if (equal? ts *fit-epoch*)    ; somebody just wrote a 0 for the timestamp!
            (when current-timestamp
              (set! record (cons (cons 'timestamp current-timestamp) record)))
            (set! current-timestamp
                  (if current-timestamp (max ts current-timestamp) ts))))

      (unless start-timestamp
        (set! start-timestamp current-timestamp))

      (let ((st (dict-ref record 'start-time #f)))
        (when (and (or (not st) (equal? st *fit-epoch*)) current-timestamp)
          (set! record (cons (cons 'start-time current-timestamp) record))))

      ;; If the record has a compressed-timestamp, add a real timestamp field
      ;; to it.
      (if (and current-timestamp (assq 'compressed-timestamp record))
          ;; NOTE: the compressed timestamp is 5 bits (0-31) and it replaces
          ;; the bottom 5 bits of the current timestamp, we DON'T simply add
          ;; the offset to the current timestamp.  To make things more
          ;; complicated, we also need to account for rollover.
          (let* ((offset (cdr (assq 'compressed-timestamp record)))
                 (bottom-bits (bitwise-and current-timestamp #x1F))
                 (new-ts (if (>= offset bottom-bits)
                             (+ (- current-timestamp bottom-bits) offset)
                             ;; timestamp rolled over, take that into account
                             (+ (- current-timestamp bottom-bits) offset #x20))))
            (cons (cons 'timestamp new-ts) record))
          record))

    (define/public (get-start-timestamp) start-timestamp)
    (define/public (get-current-timestamp) current-timestamp)

    ;; These methods need to be overriden to do something useful
    (define/public (on-file-id file-id) #f)
    (define/public (on-file-creator creator) #f)
    (define/public (on-activity activity) #f)
    (define/public (on-session session) #f)
    (define/public (on-record record) #f)
    (define/public (on-length length) #f)
    (define/public (on-lap lap) #f)
    (define/public (on-device-info device-info) #f)
    (define/public (on-location location) #f)
    (define/public (on-workout workout) #f)
    (define/public (on-workout-step workout-step) #f)
    (define/public (on-sport sport) #f)
    (define/public (on-hrv data) #f)
    (define/public (on-developer-data-id data) #f)
    (define/public (on-field-description data) #f)
    (define/public (on-training-file data) #f)

    ;; NOTE: on-activity and on-session are also events, so the user could
    ;; call on-event for those as well if needed.  this could be important if
    ;; timer-start/timer-stop events are tracked.
    (define/public (on-event event) #f)
    (define/public (on-other type data) #f)
    ;; ----------------------------------------------------------------------

    (define/public (dispatch message-type record)
      ;; Dispatch RECORD to one of the "on-..." methods depending on the
      ;; MESSAGE-TYPE.
      (let ((record (update-timestamp record)))
        ;; Remove all fields that have no values (they have #f as the value)
        (set! record (filter cdr record))

        (cond ((eq? message-type 'file-id) (on-file-id record))
              ((eq? message-type 'file-creator) (on-file-creator record))
              ((eq? message-type 'session) (on-session record))
              ((eq? message-type 'lap) (on-lap record))
              ((eq? message-type 'length) (on-length record))
              ((eq? message-type 'record) (on-record record))
              ((eq? message-type 'activity) (on-activity record))
              ((eq? message-type 'device-info) (on-device-info record))
              ((eq? message-type 'event) (on-event record))
              ((eq? message-type 'location) (on-location record))
              ((eq? message-type 'workout) (on-workout record))
              ((eq? message-type 'workout-step) (on-workout-step record))
              ((eq? message-type 'sport) (on-sport record))
              ((eq? message-type 'hrv) (on-hrv record))
              ((eq? message-type 'developer-data-id) (on-developer-data-id record))
              ((eq? message-type 'field-description) (on-field-description record))
              ((eq? message-type 'training-file) (on-training-file record))
              (#t (on-other message-type record)))))

    ))


;;..................................................... activity-builder ....


(define activity-builder%
  ;; Build an activity from a FIT file.  An instance of this class can be used
  ;; as an event dispatcher to `read-fit-records' and the activity can be
  ;; obtained at the end by calling collect-activity.
  (class fit-event-dispatcher% (init) (super-new)

    (inherit get-start-timestamp)
    (inherit get-current-timestamp)

    (define activity-timestamp #f)
    (define activity-guid #f)

    (define sessions '())
    (define laps '())
    (define lengths '())
    (define records '())
    (define devices '())
    (define sport '())
    (define training-file '())

    ;; FIT 2.0 allows "developer" fields, these hold the definitions, for
    ;; referencing the dev fields in trackpoint data.
    (define developer-data-ids '())
    (define field-descriptions '())

    (define display-next-record #f)
    (define timer-stopped #f)
    (define timer-stop-timestamp 0)

    ;; Multiplier to convert angles from the FIT representation to degrees.
    (define angle-mult (/ 360 256))

    (define (extract-angle record field index)
      (let ((pp (dict-ref record field #f)))
        (if (and pp (vector-ref pp index))
            (* (vector-ref pp index) angle-mult)
            #f)))

    ;; Some fields in FIT records are inconvenient to use so we process them
    ;; somwehat.  This is a table containing a field name and a function to
    ;; obtain a value for that field.
    (define mappings
      `(;; Ensure the record has a start-time timestamp, borrow it from the
        ;; 'timestamp' value if needed.
        (start-time . ,(lambda (t) (or (dict-ref t 'start-time #f) (dict-ref t 'timestamp #f))))

        ;; cadences (including AVG and MAX) are stored as an integer plus an
        ;; optional fractional part.  We store it as a real number internally.
        ;; Also swimming candence has a different field name.
        (cadence . ,(lambda (t)
                      (let ((c (dict-ref t 'cadence #f))
                            (f (dict-ref t 'fractional-cadence #f)))
                        (if (and (number? c) (number? f))
                            (+ c f)
                            c))))
        (avg-cadence . ,(lambda (t)
                          (or (dict-ref t 'avg-swimming-cadence #f)
                              (let ((c (dict-ref t 'avg-cadence #f))
                                    (f (dict-ref t 'avg-fractional-cadence #f)))
                                (if (and (number? c) (number? f))
                                    (+ c f)
                                    c)))))
        (max-cadence . ,(lambda (t)
                          (or (dict-ref t 'max-swimming-cadence #f)
                              (let ((c (dict-ref t 'max-cadence #f))
                                    (f (dict-ref t 'max-fractional-cadence #f)))
                                (if (and (number? c) (number? f))
                                    (+ c f)
                                    c)))))

        ;; Swimming activites have a different name for total cycles.
        (total-cycles . ,(lambda (t)
                           (or (dict-ref t 'total-cycles #f) (dict-ref t 'total-strokes #f))))

        ;; Gen2 Running Dynamics introduces GCT balance, we roll it into
        ;; left-right-balance
        (left-right-balance . ,(lambda (t)
                                 (or (dict-ref t 'left-right-balance #f)
                                     (dict-ref t 'stance-time-balance #f))))

        ;; Power phase start and end values are stored as a vector of values,
        ;; we store each individual value separately.  Same for peak power
        ;; phase.
        (left-pp-start . ,(lambda (t) (extract-angle t 'left-pp 0)))
        (left-pp-end . ,(lambda (t) (extract-angle t 'left-pp 1)))

        (right-pp-start . ,(lambda (t) (extract-angle t 'right-pp 0)))
        (right-pp-end . ,(lambda (t) (extract-angle t 'right-pp 1)))

        (left-ppp-start . ,(lambda (t) (extract-angle t 'left-peak-pp 0)))
        (left-ppp-end . ,(lambda (t) (extract-angle t 'left-peak-pp 1)))

        (right-ppp-start . ,(lambda (t) (extract-angle t 'right-peak-pp 0)))
        (right-ppp-end . ,(lambda (t) (extract-angle t 'right-peak-pp 1)))

        (avg-left-pp-start . ,(lambda (t) (extract-angle t 'avg-left-pp 0)))
        (avg-left-pp-end . ,(lambda (t) (extract-angle t 'avg-left-pp 1)))

        (avg-right-pp-start . ,(lambda (t) (extract-angle t 'avg-right-pp 0)))
        (avg-right-pp-end . ,(lambda (t) (extract-angle t 'avg-right-pp 1)))

        (avg-left-ppp-start . ,(lambda (t) (extract-angle t 'avg-left-peak-pp 0)))
        (avg-left-ppp-end . ,(lambda (t)(extract-angle t 'avg-left-peak-pp 1)))

        (avg-right-ppp-start . ,(lambda (t) (extract-angle t 'avg-right-peak-pp 0)))
        (avg-right-ppp-end . ,(lambda (t) (extract-angle t 'avg-right-peak-pp 1)))))

    (define (process-fields record)
      ;; Convert some fields inside RECORD from the FIT representation to the
      ;; more convenient internal representation.

      (define new-fields
        (filter cdr (for/list ([m mappings]) (cons (car m) ((cdr m) record)))))

      (append new-fields
              (filter
               ;; Remove fields from RECORD which are already in NEW-FIELDS
               (lambda (t)
                 (not (dict-ref new-fields (car t) #f)))
               record)))

    (define/override (on-file-id file-id)
      (unless activity-guid
        ;; Some activitites contain multiple file-id messages, keep the first
        ;; one only.
        (let ((serial-number (dict-ref file-id 'serial-number #f))
              (time-created (dict-ref file-id 'time-created #f))
              (file-type (dict-ref file-id 'type #f)))
          (unless (eq? file-type 'activity)
            (raise-error (format "not an activity: ~a" file-type)))
          ;; We use the device serial and time-created as a unique identifier
          ;; for the activity.
          (set! activity-guid (format "~a-~a" serial-number time-created))))
      #t)

    (define/public (get-guid) activity-guid)

    (define/override (on-activity activity)
      (set! activity-timestamp (dict-ref activity 'timestamp #f))
      ;; nothing more to do with this one.  the activity-guid comes from the
      ;; file-id message.
      #t)

    (define/override (on-session session)
      ;; Session records can come before the lap records (Garmin Swim), so we
      ;; cannot collect the laps when we see a session.  Instead we just save
      ;; it and process it in collect-activity.

      (let ((data (process-fields session)))
        (set! data (cons (cons 'devices devices) data))
        (cond ((or (dict-ref sport 'sport #f)
                   (dict-ref session 'sport #f))
               => (lambda (v)
                    (set! data (cons (cons 'sport v) data)))))
        (cond ((or (dict-ref sport 'sub-sport #f)
                   (dict-ref session 'sub-sport #f))
               => (lambda (v)
                    (set! data (cons (cons 'sub-sport v) data)))))
        (cond ((dict-ref session 'pool-length #f) =>
                                                  (lambda (v)
                                                    (set! data (cons (cons 'pool-length v) data)))))
        (cond ((dict-ref session 'pool-length-unit #f) =>
                                                       (lambda (v)
                                                         (set! data (cons (cons 'pool-length-unit v) data)))))
        (set! devices '())
        (set! sport '())
        (set! sessions (cons data sessions)))
      #t)

    (define/override (on-record record)
      (define precord (process-fields record))
      (if (null? records)
          (set! records (cons precord records))
          ;; Check if this record has the same timestamp as the last one.
          ;; Some devices record several data points in different records with
          ;; the same timestamp.
          (let ((last-record (car records)))
            (if (equal? (dict-ref precord 'timestamp #f)
                        (dict-ref last-record 'timestamp #t))
                ;; Merge the records, as they share timestamps
                (set! records (cons (append precord last-record) (cdr records)))
                (set! records (cons precord records)))))
      #t)


    (define/override (on-length length)
      ;; (display (format "*** LENGTH ~a~%" (dict-ref length 'timestamp #f)))
      (let ((data (process-fields length)))
        (cond ((dict-ref length 'length-type #f) =>
                                                 (lambda (v)
                                                   (set! data (cons (cons 'length-type v) data)))))
        (set! lengths (cons data lengths)))
      #t)

    (define/override (on-lap lap)
      ;; Reconstructing the track points of the lap is a bit tricky and seems
      ;; to be device specific.  The Garmin Swim FIT file is contrary to the
      ;; FIT file specification.

      ;; WARNING: The FIT file specification indicates that sessions, laps and
      ;; lengths can be grouped separately from the records.  Neither the
      ;; 310XT nor the Garmin Swim do that, so assume that the `lengths' and
      ;; `records' are already present when we see the lap message.
      (let ((data (process-fields lap)))
        (set! data
              (cond ((and (null? lengths) (null? records))
                     ;; Easy case (we hope), just a lap with no aditional
                     ;; data.
                     data)
                    ((null? lengths)
                     ;; Easy case, there were no lengths.  Construct a dummy
                     ;; length and assign all records to it.  The length will
                     ;; have the same data fields (total-timer-time, etc) as
                     ;; the lap.
                     (let ((records (reverse records)))
                       (cons
                        (cons 'lengths
                              (list
                               (cons (cons 'track records) data)))
                        data)))
                    ((= (length lengths) (length records))
                     ;; The Garmin Swim generates a LENGTH record for each
                     ;; pool length and a RECORD to go with it.
                     ;; Unfortunately, it writes the record after the length
                     ;; and with the wrong timestamp.  Since there are the
                     ;; same number of lengths as records, we just pair them
                     ;; together.

                     (cons
                      (cons 'lengths
                            (reverse (map (lambda (len rec)
                                            (cons (cons 'track (list rec)) len))
                                          lengths
                                          records)))
                      data))

                    (#t
                     ;; Most generic case, use the timestamp field to assign
                     ;; records to the corresponding lengths.
                     (let ((records (sort records < #:key (lambda (e) (dict-ref e 'timestamp #f))))
                           (lengths (sort lengths < #:key (lambda (e) (dict-ref e 'timestamp #f)))))

                       (define (add-length-records length)
                         (let ((timestamp (dict-ref length 'timestamp #f)))
                           (let-values ([(our-records rest)
                                         (splitf-at records
                                                    (lambda (v)
                                                      (<= (dict-ref v 'timestamp #f) timestamp)))])
                             (set! records rest)         ; will be used by the next length
                             (cons (cons 'track our-records) length))))

                       (let ((data (cons
                                    (cons 'lengths (map add-length-records lengths))
                                    data)))
                         (when (> (length records) 0)
                           (dbglog "fit-file: remaining records after processing LENGTHS"))
                         data)))))

        (set! records '())
        (set! lengths '())
        (set! laps (cons data laps)))
      #t)

    (define/override (on-device-info device-info)
      ;; (display (format "*** DEVICE-INFO ~a~%" device-info))
      ;; (let ((index (cond ((assq 'device-index device-info) => cdr)
      ;;                    (#t #f))))
      ;;   (when index (hash-set! devices index device-info)))
      (set! devices (cons device-info devices))
      #t)

    (define/override (on-training-file tf)
      (set! training-file (cons tf training-file)))

    (define/override (on-sport data)
      (set! sport data))

    (define/override (on-event event)
      (let ((timestamp (dict-ref event 'timestamp #f))
            (e (dict-ref event 'event #f))
            (type (dict-ref event 'event-type #f)))

        (cond
          ((eq? e 'timer)
           (cond ((eq? type 'stop-all)
                  ;; (when (pair? records)
                  ;;   (display (car records))(newline))
                  (set! timer-stopped #t)
                  (set! timer-stop-timestamp timestamp))
                 ((eq? type 'start)
                  (when timer-stopped
                    (set! display-next-record #t)
                    ;; (display (format "*** PAUSE ~a seconds~%" (- timestamp timer-stop-timestamp)))
                    (set! timer-stopped #f)))
                 (#t
                  ;; (display (format "*** Unknown timer event ~a~%" event))
                  #t)))
          ((eq? e 'session)              ; not interested in these ones
           #t)
          (#t
           ;; (display (format "*** EVENT: ~a~%" event))
           )))
      #t)

    (define/override (on-developer-data-id data)
      (set! developer-data-ids (cons data developer-data-ids)))

    (define/override (on-field-description data)
      (set! field-descriptions (cons data field-descriptions)))

    (define/public (display-devices)
      (for ((v (in-list (reverse devices))))
        (display "*** ")(display v)(newline)))

    (define/public (collect-activity)

      ;; File has one session which has the same timestamp as start-time --
      ;; this means that no records/laps will be collected in it (timestamp is
      ;; supposed to mark the end of the session).  We patch the session in
      ;; this case.
      (when (= (length sessions) 1)
        (let ((the-session (car sessions)))
          (when (equal? (dict-ref the-session 'timestamp #f)
                        (dict-ref the-session 'start-time #f))
            (set! sessions (list (cons (cons 'timestamp (get-current-timestamp))
                                       the-session))))))

      (define (add-session-laps session)
        (let ((timestamp (dict-ref session 'timestamp #f)))
          (let-values ([(our-laps rest)
                        (splitf-at laps
                                   (lambda (v)
                                     (<= (dict-ref v 'timestamp #f) timestamp)))])
            (set! laps rest)            ; will be used by the next session
            (cons (cons 'laps our-laps) session))))

      (when (or (> (length records) 0)
                (> (length lengths) 0))
        (dbglog "fit-file: records and lengths without enclosing lap")
        (on-lap `((timestamp . ,(get-current-timestamp))))
        ;; Compute the summary data for the newly added lap
        (let ((new-lap (car laps)))
          (set! new-lap (append (compute-summary-data '() '() (list new-lap) '())
                                new-lap))
          (set! laps (cons new-lap (cdr laps)))))

      (set! laps (reverse laps))        ; put them in chronological order

      (let ((sessions (reverse (map add-session-laps (reverse sessions)))))
        (when (> (length laps) 0)
          (dbglog "fit-file: laps outisde a session")
          (let ((new-session (add-session-laps `((timestamp . ,(get-current-timestamp))
                                                 ('sport . 'generic)))))
            (set! new-session (append (compute-summary-data '() '() '() (list new-session))
                                      new-session))
            (set! sessions (cons new-session sessions))))

        (unless (null? devices)
          ;; extra devices found (Garmin Swim does this, add them to the
          ;; last session.  NOTE: this is a hack, we don't check if there
          ;; are other devices attached to the last session.
          (let ((last-session (car sessions)))
            (set! sessions (cons (cons (cons 'devices devices) last-session)
                                 (cdr sessions)))))

        (list
         (cons 'start-time (or activity-timestamp (get-start-timestamp)))
         (cons 'guid activity-guid)
         (cons 'developer-data-ids developer-data-ids)
         (cons 'field-descriptions field-descriptions)
         (cons 'training-file training-file)
         (cons 'sessions (reverse sessions)))))

    ))

(define (read-activity-from-file file)
  ;; Convenience function to read an activity from a file.
  (let ((stream (make-fit-data-stream file))
        (builder (new activity-builder%)))
    (read-fit-records stream builder)
    (send builder collect-activity)))

(define (fit-get-device-name device-info)
  ;; Return a convenient device name from a DEVICE-INFO record.  This function
  ;; is somewhat simplistic and will need to be made more generic, w.r.t
  ;; mapping manufacturer, product to actual product names.

  (let ((manufacturer (dict-ref device-info 'manufacturer #f))
        (product (dict-ref device-info 'product #f))
        (antdev (or (dict-ref device-info 'ant-device-type #f)
                    (dict-ref device-info 'antplus-device-type #f))))
    (cond ((eq? antdev 'stride-speed-distance) "Footpod")
          ((eq? antdev 'bike-speed-cadence) "Bike Speed-Cadence Sensor")
          ((eq? antdev 'bike-cadence) "Bike Cadence Sensor")
          ((eq? antdev 'bike-speed) "Bike Speed Sensor")
          ;; NOTE: check for a HRM-RUN sensor by manufacturer+product, as the
          ;; antdev reports it as a simple heart rate monitor.
          ((or (and (eq? manufacturer 'garmin) (eq? product 'hrm-run))
               (eq? product 'hrm-run-single-byte-product-id))
           "Heart Rate Monitor (HRM-RUN)")
          ((and (eq? manufacturer 'garmin) (eq? product 'hrm4))
           "Heart Rate Monitor (HRM4)")
          ((and (eq? manufacturer 'garmin)
                (member product '(vector vector_s vector2 vector2_s))
                (eq? antdev 'bike-power))
           "Garmin Vector (Bike Power Meter)")
          ((eq? antdev 'heart-rate) "Heart Rate Monitor")
          ((and (eq? manufacturer 'garmin) (eq? product 'fr310xt)) "Garmin Forerunner 310XT")
          ((and (eq? manufacturer 'garmin) (eq? product 'fr920xt)) "Garmin Forerunner 920XT")
          ((and (eq? manufacturer 'garmin) (eq? product 'swim)) "Garmin Swim")
          ((eq? antdev 'bike-power) "Power Meter")
          (#t (format "~a/~a/~a" manufacturer product antdev)))))


;;..................................................... fit file writing ....

(define fit-output-stream%
  ;; Helper class to write FIT files.  This is low level stuff, you need to
  ;; handle putting message defintions and messages yourself.  Most likely you
  ;; want to start up with fit-output-file% instead of this class.
  (class object% (init) (super-new)

    (struct mdef (global-id local-id size fields))

    (define header-length 14)
    (define protocol-version 16)
    (define profile-version 1322)

    (define buffer (make-bytes header-length))
    (define mark header-length)

    (define big-endian? #t)

    (define message-definitions (make-hash))

    (define (ensure-space-for n)
      ;; Make sure our buffer has space for at least n bytes
      (let ((available (- (bytes-length buffer) mark)))
        (when (< available n)
          (let ((needed (- n available)))
            (let ((nbuf (make-bytes (+ (bytes-length buffer) needed  0))))
              (bytes-copy! nbuf 0 buffer 0 (bytes-length buffer))
              (set! buffer nbuf))))))

    (define/public (put-message-definition global-id local-id definition)
      ;; Register a new message with GLOBAL-ID that will use LOCAL-ID in the
      ;; FIT file.  DEFINITION contains the list of fields to encode in the
      ;; message.  The message definition is also written to the internal
      ;; buffer.

      ;; Any previous message with the same LOCAL-ID will be overwriten (this
      ;; feature is used frequently).
      (ensure-space-for
       (+ 1 1 1 2 1 (* 3 (length definition))))
      (let ((header (bitwise-ior local-id #x40)))
        (bytes-set! buffer mark header)
        (set! mark (+ 1 mark)))

      (bytes-set! buffer mark 0)        ; reserved byte
      (set! mark (+ 1 mark))

      (bytes-set! buffer mark (if big-endian? 1 0)) ; arhitecture
      (set! mark (+ 1 mark))

      (integer->integer-bytes global-id 2 #f big-endian? buffer mark)
      (set! mark (+ 2 mark))

      (bytes-set! buffer mark (length definition))
      (set! mark (+ 1 mark))

      (let ((message-size 0))

        (for ([def (in-list definition)])
          (let ((type (get-fit-type (list-ref def 3))))
            (unless type
              (raise-error (format "bad type: ~a" (list-ref def 3))))
            (let ((fnum (list-ref def 1))
                  (size (* (list-ref def 2) (fit-type-size type)))
                  (typeid (fit-type-id type)))

              (set! message-size (+ message-size size))

              (bytes-set! buffer mark fnum)
              (bytes-set! buffer (+ mark 1) size)
              (bytes-set! buffer (+ mark 2) typeid)
              (set! mark (+ 3 mark)))))

        (hash-set! message-definitions
                   global-id
                   (mdef global-id local-id message-size definition)))
      #t)

    (define/public (put-message global-id message-data)
      ;; Write a new message with GLOBAL-ID MESSAGE-DATA contains the data for
      ;; the message.  A previous message defintion should have been written
      ;; for this GLOBAL-ID
      (let ((definition (hash-ref message-definitions global-id #f)))
        (unless definition
          (raise-error (format "undefined global message ~a" global-id)))
        (ensure-space-for (+ 1 (mdef-size definition)))
        (bytes-set! buffer mark (mdef-local-id definition))
        (set! mark (+ 1 mark))
        (for ([field (in-list (mdef-fields definition))])
          (let ((value (dict-ref message-data (list-ref field 0) #f))
                (type (get-fit-type (list-ref field 3))))
            (set! mark (write-fit-value buffer mark type value big-endian?)))))
      #t)

    (define/public (get-fit-data)
      ;; Fill in the FIT header, compute the CRC and return the FIT data.
      (bytes-set! buffer 0 header-length)
      (bytes-set! buffer 1 protocol-version)
      (integer->integer-bytes profile-version 2 #f #f buffer 2)
      (let ((data-length (- mark header-length)))
        (integer->integer-bytes data-length 4 #f #f buffer 4))
      (bytes-copy! buffer 8 (string->bytes/utf-8 ".FIT"))
      (integer->integer-bytes
       (compute-crc buffer 0 12)
       2 #f #f buffer 12)
      ;; Put the CRC at the end
      (let ((crc (compute-crc buffer 0 mark)))
        (ensure-space-for 2)
        (integer->integer-bytes crc 2 #f #f buffer mark))
      (subbytes buffer 0 (+ 2 mark)))

    ))



;;..................................................... fit-output-file% ....

(define fit-output-file%
  ;; Create a FIT output stream with the file-id and file-creator messages
  ;; already filled in.  These two messages need to be present in all fit
  ;; files anyway.  Sensible defaults are provided for all fields, but can be
  ;; overriden.  The only parameter that needs a valid value is the FILE-TYPE.

  (class fit-output-stream% (init)
    (init-field
     [file-type #f]
     [manufacturer 1]                   ; Garmin
     [product 65534]                    ; Connect
     [serial-number 1]
     [time-created #f]                  ; if #f, will be updated to current time
     [number #f]
     [hardware-version #f]
     [software-version 1])
    (super-new)
    (inherit put-message-definition put-message)

    (define file-id-message 0)          ; global meessage number
    (define file-id-definition
      '((type 0 1 enum)
        (manufacturer 1 1 uint16)
        (product 2 1 uint16)
        (serial-number 3 1 uint32z)
        (time-created 4 1 uint32)       ; FIT epoch
        (number 5 1 uint16)))

    (define file-creator-message 49)    ; global message number
    (define file-creator-definition
      '((software-version 0 1 uint16)
        (hardware-version 1 1 uint8)))

    ;; Write the file-id and file-creator messages immediately.  Both messages
    ;; use local-id 0.  Local-id 0 should also be available immediately, as
    ;; these messages will not be wrtten again.

    (put-message-definition file-id-message 0 file-id-definition)
    (put-message
     file-id-message
     `((type . ,file-type)
       (manufacturer . ,manufacturer)
       (product . ,product)
       (serial-number . ,serial-number)
       (time-created . ,(unix-time->fit-time (or time-created (current-seconds))))
       (number . ,number)))

    (put-message-definition file-creator-message 0 file-creator-definition)
    (put-message
     file-creator-message
     `((software-version . ,software-version)
       (hardware-version . ,hardware-version)))

    ))



;;.................................................... fit-workout-file% ....

(define (mk-fit-string str size)
  ;; Convert STR, a string into a vector of SIZE bytes, ready to be written
  ;; into a FIT file.  Strings in FIT files have fixed length and are 0
  ;; padded.
  (let ((result (make-vector size 0)))
    (let loop ((idx 0))
      ;; NOTE: we make the string 0 terminated always even though the fit
      ;; standard does not require us
      (when (< idx (min (- size 1) (string-length str)))
        (vector-set! result idx (char->integer (string-ref str idx)))
        (loop (+ idx 1))))
    result))

(define fit-workout-file%
  ;; Create a workout fit file. Note that this is still a fairly low level
  ;; interface for creating workouts.
  (class fit-output-file% (init)
    (init-field name sport)
    (super-new [file-type 5])

    (inherit put-message-definition put-message)

    ;; The index of each workout step.  This is automatically managed by the
    ;; class, but reading it is usefull when adding repeat steps, as they need
    ;; the message index to jump to.
    (define message-index 0)
    (define/public (get-next-message-index) message-index)

    (define workout-message 26)
    (define workout-definition          ; global message 26
      '((name 8 15 string)
        (sport 4 1 enum)
        (capabilities 5 1 uint32z)
        (num-steps 6 1 uint16)))

    (define workout-step-message 27)
    (define workout-step-definition     ; global message 27
      '((message-index 254 1 uint16)
        ;;(name 0 10 string)
        (duration-type 1 1 enum)
        (duration-value 2 1 uint32)
        (target-type 3 1 enum)
        (target-value 4 1 uint32)
        (custom-target-value-low 5 1 uint32)
        (custom-target-value-high 6 1 uint32)
        (intensity 7 1 enum)))

    ;; List of steps in the workout, as added by `add-step'.  Note that they
    ;; are stored in reverse order.
    (define workout-steps '())

    (define (write-workout)
      ;; Write the entire workout to the FIT file.
      (put-message-definition workout-message 0 workout-definition)
      (put-message
       workout-message
       `((sport . ,sport)
         (capabilities . 32)
         (num-steps . ,(length workout-steps))
         (name . ,(mk-fit-string name 15))))
      (put-message-definition workout-step-message 0 workout-step-definition)
      (for ([step (in-list (reverse workout-steps))])
        (put-message workout-step-message step)))

    (define/public (add-step step)
      ;; Add a workout step.  This is an A-LIST corresponding to
      ;; `workout-step-definition' with the step data.
      (set! workout-steps
            (cons (cons (cons 'message-index message-index) step) workout-steps))
      (set! message-index (+ 1 message-index)))

    (define/override (get-fit-data)
      ;; Get the FIT data corresponding to the workout.  This also writes the
      ;; workout to the file.
      (write-workout)
      (super get-fit-data))

    ))


;;...................................................... fit-sport-file% ....

(define fit-sport-file%
  ;; Create a fit sport file (this contains zone definitions for various
  ;; sports).
  (class fit-output-file%
    (init)
    (init-field [sport 0] [sub-sport 0]
                [max-hr #f] [ftp #f]
                [hr-zones #f]
                [speed-zones #f]
                [power-zones #f])
    (super-new [file-type 3])           ; sport file
    (inherit put-message-definition put-message)

    (define zones-target-message 7)
    (define zones-target-definition
      '((max-heart-rate 1 1 uint8)
        (functional-threshold-power 3 1 uint16)
        (hr-calculation-type 5 1 enum)
        (power-calculation-type 7 1 enum)))

    (define sport-message 12)
    (define sport-message-definition
      '((sport 0 1 enum)
        (sub-sport 1 1 enum)))

    (define hr-zone-message 8)
    (define hr-zone-definition
      '((message-index 254 1 uint16)
        (high-bpm 1 1 uint8)
        ;;(name 2 10 string)
        ))

    (define speed-zone-message 53)
    (define speed-zone-definition
      '((message-index 254 1 uint16)
        (high-value 1 1 uint16)
        ;;(name 1 10 string)
        ))

    (define power-zone-message 9)
    (define power-zone-definition
      '((message-index 254 1 uint16)
        (high-value 1 1 uint16)
        ;;(name 2 10 string)
        ))

    (define (write-settings)
      (put-message-definition zones-target-message 0 zones-target-definition)
      (put-message
       zones-target-message
       `((max-heart-rate . ,(if max-hr (exact-round max-hr) #f))
         (functional-threshold-power . ,(if ftp (exact-round ftp) #f))
         (hr-calculation-type . ,(if hr-zones 0 #f))
         (power-calculation-type . ,(if power-zones 0 #f))))
      (put-message-definition sport-message 0 sport-message-definition)
      (put-message
       sport-message
       `((sport . ,sport)
         (sub-sport . ,sub-sport)))
      (when hr-zones
        (put-message-definition hr-zone-message 0 hr-zone-definition)
        (for ([message-index (in-range (length hr-zones))]
              [val (in-list hr-zones)])
          (put-message
           hr-zone-message
           `((message-index . ,message-index)
             (high-bpm . ,(exact-round val))
             (name . ,(mk-fit-string "" 10))))))
      (when power-zones
        (put-message-definition power-zone-message 0 power-zone-definition)
        (for ([message-index (in-range (length power-zones))]
              [val (in-list power-zones)])
          (put-message
           power-zone-message
           `((message-index . ,message-index)
             (high-value . ,(exact-round val))
             (name . ,(mk-fit-string "" 10))))))
      (when speed-zones
        (put-message-definition speed-zone-message 0 speed-zone-definition)
        (for ([message-index (in-range (length speed-zones))]
              [val (in-list speed-zones)])
          (put-message
           speed-zone-message
           `((message-index . ,message-index)
             (high-value . ,(exact-round (* 1000.0 val)))
             (name . ,(mk-fit-string "" 10)))))))

    (define/override (get-fit-data)
      (write-settings)
      (super get-fit-data))

    ))


;;................................................... fit-settings-file% ....

(define fit-settings-file%
  ;; Create a fit settings file (this atthlete info such as heiht, body
  ;; weight, etc)
  (class fit-output-file%
    (init)
    (init-field [date-of-birth #f]
                [gender #f]
                [weight #f]
                [height #f]
                [activity-class #f]
                [collect-hrv-data? #f])
    (super-new [file-type 2])           ; settings
    (inherit put-message-definition put-message)

    (define user-profile-message 3)
    (define user-profile-message-definition
      '((gender 1 1 enum)
        (age 2 1 uint8)
        (height 3 1 uint8)
        (weight 4 1 uint16)
        (activity-class 17 1 enum)
        (birth-year 24 1 uint8)))

    (define hrm-profile-message 4)
    (define hrm-profile-message-definition
      '((log-hrv 2 1 enum)))

    (define (write-settings)

      (define age
        (and date-of-birth
             (exact-round (/ (- (current-seconds) date-of-birth) (* 3600 24 365)))))

      (define birth-year
        (and date-of-birth
             (let ((date (seconds->date date-of-birth)))
               (- (date-year date) 1900))))

      (put-message-definition user-profile-message 0 user-profile-message-definition)
      (put-message
       user-profile-message
       `((gender . ,gender)
         (age . ,age)
         (height . ,(if height (exact-round (* height 100)) #f))
         (weight . ,(if weight (exact-round (* weight 10)) #f))
         (activity-class . ,(if activity-class (exact-round (* activity-class 10)) #f))
         (birth-year . ,birth-year)))

      (put-message-definition hrm-profile-message 0 hrm-profile-message-definition)
      (put-message
       hrm-profile-message
       `((log-hrv . ,(if collect-hrv-data? 1 0)))))

    (define/override (get-fit-data)
      (write-settings)
      (super get-fit-data))

    ))
