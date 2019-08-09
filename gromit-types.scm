;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; gromit-types - Types for Attributes and Properties.
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
;;; This module defines the built-in types.
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
;;; Andy Bennett <andyjpb@register-dynamics.co.uk>, 2019/07/02
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(module gromit-types
	(; Macros
	 define-attribute-type

	 ; Built-in Types
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
	 )


(import chicken scheme)

; Units - http://api.call-cc.org/doc/chicken/language
(use data-structures)

; Eggs - http://wiki.call-cc.org/chicken-projects/egg-index-4.html
(use srfi-19)
(use-for-syntax matchable)

(use            gromit-lolevel gromit-consistency)
(use-for-syntax gromit-lolevel)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Helper Macros

;; Attribute Types

(define-syntax define-attribute-type
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-attribute-type NAME COLUMN-TYPE ARGS ...)
	 (let* ((NAME*        (strip-syntax NAME))
		(syntax-NAME  (symbol-append 'syntax- NAME*))
		(COLUMN-TYPE* (strip-syntax COLUMN-TYPE)))
	   `(begin
	      (define-for-syntax ,syntax-NAME (make-attribute-type* ',NAME* ,COLUMN-TYPE* 'macrotime))
	      (define            ,NAME        (make-attribute-type* ',NAME* ,COLUMN-TYPE* 'runtime   ,@(inject ARGS))) )))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Helpers

(define NULL '())


(define (none msg)
  (lambda _
    (assert #f msg)))


(define constantly-NULL
  (constantly NULL))


; A common serialiser for use where sql-de-lite understands the internal
; representation but we want to coerce #f values to NULL.
(define (value-or-NULL v)
  (if v v NULL))


; A common deserialiser where sql-de-lite gives us the correct internal
; representation but we want to coerce NULL values to #f.
(define (value-or-false v)
  (if (null? v) #f v))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Built-in Types

; An integer that the user can set to any valid value.
(define-attribute-type integer 'INTEGER
		       serialiser:   value-or-NULL
		       deserialiser: value-or-false
		       initialiser:  constantly-NULL
		       setter:       (lambda (i)
				       (cond
					 ((integer? i) i)
					 ((eq? #f i)   i)
					 (else
					   (assert #f (conc "integer field expected an integer or #f. We got " i "!")))))
		       getter:       identity)


; A string that the user can set to any valid value.
(define-attribute-type text 'TEXT
		       serialiser:   value-or-NULL
		       deserialiser: value-or-false
		       initialiser:  constantly-NULL
		       setter:       (lambda (t)
				       (cond
					 ((string? t) t)
					 ((eq? #f t)  t)
					 (else
					   (assert #f (conc "text field expected a string or #f. We got " t "!")))))
		       getter:       identity)


; A blob that the user can set to any valid value.
(define-attribute-type blob 'BLOB
		       serialiser:   value-or-NULL
		       deserialiser: value-or-false
		       initialiser:  constantly-NULL
		       setter:       (lambda (b)
				       (cond
					 ((blob? b)  b)
					 ((eq? #f b) b)
					 (else
					   (assert #f (conc "blob field expected a blob or #f. We got " b "!")))))
		       getter:       identity)


(define-attribute-type boolean 'INTEGER
		       serialiser:   (lambda (b)
				       (if b 1 0))
		       deserialiser: (lambda (b)
				       ; Don't use NULL to represent #f as then
				       ; boolean fields cannot be marked as
				       ; required.
				       (cond
					 ((null? b) #f)
					 ((eq? 0 b) #f)
					 ((eq? 1 b) #t)))
		       initialiser:  (constantly #f)
		       setter:       (lambda (b)
				       (cond
					 ((eq? #t b) b)
					 ((eq? #f b) b)
					 (else
					   (assert #f (conc "boolean field expected #t or #f. We got " b "!")))))
		       getter:       identity)


; A symbol that the user can set to any valid value.
(define-attribute-type symbol 'TEXT
		       serialiser:   (lambda (s)
				       (if s
					 (symbol->string s)
					 NULL))
		       deserialiser: (lambda (s)
				       (if (null? s)
					 #f
					 (string->symbol s)))
		       initialiser:  constantly-NULL
		       setter:       (lambda (s)
				       (cond
					 ((symbol? s) s)
					 ((eq? #f s)  s)
					 (else
					   (assert #f (conc "symbol field expected a symbol or #f. We got " s "!")))))
		       getter:       identity)


; The wallclock time in UTC when the field was last serialised.
; This field cannot be set by the user.
(define-attribute-type wallclock 'INTEGER
		       serialiser:   (lambda (_)
				       (parameterize ((local-timezone-locale (utc-timezone-locale)))
					 (current-seconds)))
		       deserialiser: value-or-false
		       initialiser:  values
		       setter:       (none "Fields of type wallclock cannot be set!")
		       getter:       identity)


; The node-id of the node that last serialised this field.
; This field cannot be set by the user.
; This is named node-id/current-node because node-id is a type.
; Contrast with current-user/{node-id,seqno}.
(define-attribute-type  node-id/current-node 'INTEGER
			serialiser:   (lambda (_) gromit-node-id) ; (constantly ...) would capture the value of gromit-node-id.
			deserialiser: value-or-false
			initialiser:  values
			setter:       (none "Fields of type node-id/current-node cannot be set!")
			getter:       identity)


; The next gromit timestamp on the node that last serialised this field.
; This field cannot be set by the user.
(define-attribute-type timestamp/next 'INTEGER
		       serialiser:   (lambda (_) (gromit-next-timestamp))
		       deserialiser: value-or-false
		       initialiser:  values
		       setter:       (none "Fields of type timestamp/next cannot be set!")
		       getter:       identity)


; The node-id of the object-id of the current user at the time this field was
; last serialised.
; This field cannot be set by the user.
; This is named current-user/node-id because current-user is an object-id.
; Contrast with node-id/current-node.
(define-attribute-type current-user/node-id 'INTEGER
		       serialiser:   (lambda (_)
				       (let ((current-user (current-gromit-user)))
					 (assert current-user (conc "Field of type current-user/node-id expected current-gromit-user to contain an object-id. We got " current-user "!"))
				       (object-id-node-id current-user)))
		       deserialiser: value-or-false
		       initialiser:  values
		       setter:       (none "Fields of type current-user/node-id cannot be set!")
		       getter:       identity)


; The seqno of the object-id of the current user at the time this field was
; last serialised.
; This field cannot be set by the user.
; This is named current-user/seqno because current-user is an object-id.
; Contrast with node-id/current-node.
(define-attribute-type current-user/seqno 'INTEGER
		       serialiser:   (lambda (_)
				       (let ((current-user (current-gromit-user)))
					 (assert current-user (conc "Field of type current-user/seqno expected current-gromit-user to contain an object-id. We got " current-user "!"))
					 (object-id-seqno current-user)))
		       deserialiser: value-or-false
		       initialiser:  values
		       setter:       (none "Fields of type current-user/seqno cannot be set!")
		       getter:       identity)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
)

