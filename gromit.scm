;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; gromit - A Graph-like Object Persistence Toolkit.
;;;
;;; gromit manages a set of typed objects that can be persisted to an SQLite
;;; database. The objects can be linked together with labelled edges.
;;;
;;; gromit objects are made up of attributes and properties. Attributes are
;;; simple, single valued data fields. Properties are lists of sets of fields.
;;; The user can select from the built-in data types for fields or define their
;;; own. Edges are much simpler: they specify a start point, an end point, have
;;; a label and are directional.
;;;
;;;
;;;  Copyright (C) 2019, Andy Bennett, Register Dynamics Limited.
;;;  All rights reserved.
;;;
;;;  Redistribution and use in source and binary forms, with or without
;;;  modification, are permitted provided that the following conditions are met:
;;;
;;;  Redistributions of source code must retain the above copyright notice, this
;;;  list of conditions and the following disclaimer.
;;;  Redistributions in binary form must reproduce the above copyright notice,
;;;  this list of conditions and the following disclaimer in the documentation
;;;  and/or other materials provided with the distribution.
;;;  Neither the name of the author nor the names of its contributors may be
;;;  used to endorse or promote products derived from this software without
;;;  specific prior written permission.
;;;
;;;  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
;;;  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
;;;  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
;;;  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
;;;  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
;;;  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
;;;  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
;;;  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
;;;  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
;;;  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
;;;  POSSIBILITY OF SUCH DAMAGE.
;;;
;;; Andy Bennett <andyjpb@register-dynamics.co.uk>, 2019/06/25
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(module gromit
	(; Object IDs
	 make-object-id

	 ; Attributes
	 define-attribute-type

	 ; Properties
	 define-property-constructor
	 define-property-type

	 ; Object Metadata
	 create-table-%metadata
	 object-metadata-name
	 object-metadata-description
	 object-metadata-version
	 object-metadata-created
	 object-metadata-created-by
	 object-metadata-last-modified
	 object-metadata-last-modified-by

	 ; Objects
	 object-id
	 object-type
	 define-object-constructor
	 define-object-finder
	 (syntax: define-object-type %metadata)

	 ; Built-in Field Types
	 integer
	 text
	 blob
	 boolean
	 symbol
	 wallclock
	 node-id/current-node
	 timestamp/next
	 current-user/node-id
	 current-user/seqno

	 ; Runtime
	 current-gromit-db
	 open-gromit-database
	 current-gromit-user
	 with-gromit-read-transaction
	 with-gromit-write-transaction
	 with-gromit-nested-transaction
	 with-gromit-nested-write-transaction
	 )

(import chicken scheme)
(import-for-syntax chicken)

; Units - http://api.call-cc.org/doc/chicken/language
(use data-structures extras srfi-1)

; Eggs - http://wiki.call-cc.org/chicken-projects/egg-index-4.html
(use matchable)
(use-for-syntax matchable)

(use            gromit-lolevel gromit-types gromit-records gromit-sqlite)
(use-for-syntax gromit-lolevel gromit-types)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Compile Time Macros
;;; This is the language that the user uses to specify their objects and
;;; properties.

;; Properties

(define-syntax define-property-type
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-property-type NAME ((? (cut compare <> 'quote)) TABLE) FIELDS ...)
	 (let* ((NAME*        (strip-syntax NAME))
		(TABLE*       (strip-syntax TABLE))
		(syntax-NAME  (symbol-append 'syntax- NAME*))
		(rtd          (make-property-type* NAME TABLE* (strip-syntax (fields->list macrotime FIELDS))))
		(NAME?        (symbol-append NAME* '?))
		(make-NAME    (symbol-append 'make-    NAME*))
		(create-table-NAME (symbol-append 'create-table- NAME*))
		(%create-NAME (symbol-append '%create- NAME*)) ; No ! because it doesn't mutate existing state.
		(%save-NAME!  (symbol-append '%save-   NAME* '!))
		(read-NAME    (symbol-append 'read-    NAME* ))
		(find-NAME    (symbol-append 'find-    NAME*)))
	   `(begin
	      (define-for-syntax                   ,syntax-NAME  (make-property-type* ',NAME* ',TABLE* (fields->list macrotime ,FIELDS)))
	      (define                              ,NAME*        (make-property-type* ',NAME* ',TABLE* (fields->list runtime   ,FIELDS)))
	      (define                              ,NAME?        (make-property-predicate ,NAME*))
	      (define-property-accessors+modifiers ,NAME*        ,rtd)
	      (define-default-property-constructor ,make-NAME    ,NAME* ,rtd)
	      (define-property-create-table/sqlite ,create-table-NAME ,NAME* ,rtd)
	      (define-property-allocator/sqlite    ,%create-NAME ,NAME* ,NAME? ,rtd)
	      (define-property-updater/sqlite      ,%save-NAME!  ,NAME* ,NAME? ,rtd)
	      (define-property-reader/sqlite       ,read-NAME   ,NAME* ,rtd))))))))


;; Object Metadata

(define-property-type %metadata 'objects
  (name                     text)
  (description              text)
  (version-ts               timestamp/next       required)
  (version-node-id          node-id/current-node required)
  (created                  wallclock            required constant)
  (created-by-node-id       current-user/node-id required constant)
  (created-by-seqno         current-user/seqno   required constant)
  (last-modified            wallclock            required)
  (last-modified-by-node-id current-user/node-id required)
  (last-modified-by-seqno   current-user/seqno   required))


;; Objects

(define-syntax define-object-type
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-object-type NAME ((? (cut compare <> 'quote)) TABLE) FIELDS ...)
	 (let* ((NAME*       (strip-syntax NAME))
		(TABLE*      (strip-syntax TABLE))
		(syntax-NAME (symbol-append 'syntax- NAME*))
		(rtd         (make-object-type* NAME TABLE* syntax-%metadata (strip-syntax (fields->list macrotime FIELDS))))
		(NAME?       (symbol-append NAME* '?))
		(make-NAME   (symbol-append 'make-   NAME*))
		(create-table-NAME (symbol-append 'create-table- NAME*))
		(create-NAME (symbol-append 'create- NAME*)) ; No ! because it doesn't mutate existing state.
		(save-NAME!  (symbol-append 'save-   NAME* '!))
		(read-NAME   (symbol-append 'read-   NAME* ))
		(list-NAME   (symbol-append 'list-   NAME*)))
	   `(begin
	      (define-for-syntax                 ,syntax-NAME (make-object-type*    ',NAME* ',TABLE* syntax-%metadata (fields->list macrotime ,FIELDS)))
	      (define                            ,NAME*       (make-object-type*    ',NAME* ',TABLE* %metadata (fields->list runtime   ,FIELDS)))
	      (define                            ,NAME?       (make-object-predicate ,NAME*))
	      (define-object-accessors+modifiers ,NAME*       ,rtd)
	      (define-default-object-constructor ,make-NAME   ,NAME* ,rtd)
	      (define-object-create-table/sqlite ,create-table-NAME ,NAME* ,rtd)
	      (define-object-allocator/sqlite    ,create-NAME ,NAME* ,NAME? ,rtd)
	      (define-object-updater/sqlite      ,save-NAME!  ,NAME* ,NAME? ,rtd)
	      (define-object-reader/sqlite       ,read-NAME   ,NAME* ,rtd))))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Object Metadata

(define (object-metadata-name object #!optional (value no-value))
  (%metadata-name
    (object-metadata object)
    value))


(define (object-metadata-description object #!optional (value no-value))
  (%metadata-description
    (object-metadata object)
    value))


(define object-metadata-version
  (lambda (object)
    (let ((metadata (object-metadata object)))
      (vector
	(%metadata-version-ts      metadata)
	(%metadata-version-node-id metadata)))))


(define object-metadata-created
  (compose %metadata-created
	   object-metadata))


(define object-metadata-created-by
  (lambda (object)
    (let ((metadata (object-metadata object)))
      (vector
	(%metadata-created-by-node-id metadata)
	(%metadata-created-by-seqno   metadata)))))


(define object-metadata-last-modified
  (compose %metadata-last-modified
	   object-metadata))

(define object-metadata-last-modified-by
  (lambda (object)
    (let ((metadata (object-metadata object)))
      (vector
	(%metadata-last-modified-by-node-id metadata)
	(%metadata-last-modified-by-seqno   metadata)))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
)

