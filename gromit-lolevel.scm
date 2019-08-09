;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; gromit-lolevel - Stuff that's required at expansion time and run time.
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
;;; Andy Bennett <andyjpb@register-dynamics.co.uk>, 2019/07/01
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(module gromit-lolevel
	(; Helper Macros
	 fields->list
	 field-type->syntax-type*

	 ; Object IDs
	 make-object-id
	 object-id-node-id
	 object-id-seqno

	 ; Current User
	 current-gromit-user

	 ; Attribute Types
	 make-attribute-type*
	 attribute-type-name
	 attribute-type-column-type
	 attribute-type-serialiser
	 attribute-type-deserialiser
	 attribute-type-initialiser
	 attribute-type-setter
	 attribute-type-getter

	 ; Attribute Specs
	 attribute-spec?
	 attribute-spec-field-idx
	 attribute-spec-type
	 attribute-spec-name
	 attribute-spec-required?
	 attribute-spec-constant?
	 attribute-spec-key-part

	 ; Property Types
	 property-spec?
	 make-property-type*
	 property-type-name
	 property-type-table
	 property-type-key-specs
	 property-type-attribute-specs

	 ; Property Specs
	 property-spec-field-idx
	 property-spec-type
	 property-spec-name

	 ; Object Fields Specs
	 field-spec-name

	 ; Object Types
	 make-object-type*
	 object-type-name
	 object-type-table
	 object-type-metadata-type
	 object-type-attribute-specs
	 object-type-property-specs
	 object-type-field-names
	 )

(import chicken scheme)

; Units - http://api.call-cc.org/doc/chicken/language
(use data-structures extras srfi-1)

; Eggs - http://wiki.call-cc.org/chicken-projects/egg-index-4.html
(use matchable)
(use-for-syntax matchable)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Helper Macros

; Extract and remove the named option from a field option list.
; Returns a list of matches.
(define-syntax option!
  (syntax-rules ()
    ((option! option options)
     (receive (match rest)
	      (partition
		(lambda (o)
		  (if (list? o)
		    (eqv? (car o) option)
		    (eqv? o option)))
		options)
	      (set! options rest)
	      match))))


; Converts a runtime field name such as integer or password into the name of
; the field definition available at expansion time such as syntax-integer or
; syntax-password.
(define (field-type->syntax-type* symbol)
  (let* ((symbol*       (strip-syntax symbol))
	 (syntax-symbol (symbol-append 'syntax- symbol*)))
    syntax-symbol))


(define-syntax field-type->syntax-type
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((field-type->syntax-type NAME)
	 (let* ((NAME*       (inject NAME))
		(NAME*       (strip-syntax NAME*))
		(syntax-NAME (symbol-append 'syntax- NAME*)))
	   syntax-NAME))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; External Helper Macros

; Take a field list as supplied from define-object-type or define-property-type
; and quote it and evaluate it properly.
(define-syntax fields->list
  (syntax-rules (runtime macrotime)

    ((fields->list runtime ((name type flags ...) ...))
     `((name ,type flags ...) ...))

    ((fields->list macrotime ((name type flags ...) ...))
     `((name ,(field-type->syntax-type type) flags ...) ...))

    ((fields->list macrotime fields)
     (map
       (match-lambda
	 ((name type . flags)
	  (let ((type (##sys#slot (symbol-append 'syntax- (strip-syntax type)) 0)))
	    `(,name ,type ,@flags))))
       fields))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Object IDs

(define (make-object-id node-id seqno)
  (vector node-id seqno))


(define (object-id-node-id object-id)
  (vector-ref object-id 0))


(define (object-id-seqno object-id)
  (vector-ref object-id 1))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Information about the current user

; An object ID that represents the current gromit user.
(define current-gromit-user (make-parameter #f))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Structures to hold the schema information


;; Attribute Types
;; This is allocated when define-field is used.

(define-record attribute-type
	       name
	       column-type
	       serialiser
	       deserialiser
	       initialiser
	       setter
	       getter)


(define-record-printer (attribute-type ft port)
  (fprintf port "#<gromit-attribute-type ~S ~S>"
	   (attribute-type-name        ft)
	   (attribute-type-column-type ft)))


; Creates a new type of attribute.
;
;  column-type : The type of the underlying column that is used for the
;                serialised value when it is stored in the database.
;
;   serialiser : Called when an object or property is being saved back to the
;                database. A procedure of one argument that receives the
;                current value of the attribute in memory and returns a
;                serialised version of the attribute suitable for storing in
;                SQLite. Each time an attempt is made to write the object or
;                property back to the database, this is called no more than
;                once.
;
; deserialiser : Called when an object or property is being read from the
;                database. A procedure of one argument that receives the value
;                of the attribute from the database and returns a deserialied
;                version of the attribute suitable for storing in memory. Each
;                time an attempt is makde to read the object or property back
;                from the database, this is called no more than once.
;
; initialiser  : Called when a new object that contains this attribute is being
;                allocated. A procedure of no arguments that must return the
;                initial value for the attribute. The value returned must be a
;                deserialised value suitable for storing in memory.
;                Defaults to #f.
;                This is called the first time an attempt is made to allocate
;                the object or property. If the allocation subsequently fails,
;                it will not be called again.
;
;       setter : Called when user code tries to set the value of an attribute.
;                A procedure of one argument that receives the value supplied
;                by the user as used in CHICKEN code and returns the value
;                suitable for storing in memory. Usually the identity procedure
;                or a guard. If the attribute is marked as constant by the
;                structure that uses it then the setter procedure is only
;                called before the structure has been written to disk for the
;                first time.
;                Defaults to the identity procedure.
;
;       getter : Called when user code tries to get the value of an attribute.
;                A procedure of one argument that receives the value stored in
;                memory and returns a value for the user, suitable for use in
;                CHICKEN code.
;                Defaults to the identity procedure.
;
(define (make-attribute-type* name
			      column-type
			      phase
			      #!key
			      serialiser
			      deserialiser
			      initialiser
			      setter
			      getter)

  (case phase
    ((runtime)
     (assert (procedure? serialiser)   (conc "make-attribute-type: serialiser must be a procedure. We got " serialiser "!"))
     (assert (procedure? deserialiser) (conc "make-attribute-type: deserialiser must be a procedure. We got " deserialiser "!"))
     (assert (procedure? initialiser)  (conc "make-attribute-type: initialiser must be a procedure. We got " initialiser "!"))
     (assert (procedure? setter)       (conc "make-attribute-type: setter must be a procedure. We got " setter "!"))
     (assert (procedure? getter)       (conc "make-attribute-type: getter must be a procedure. We got " getter "!"))

     (make-attribute-type name column-type serialiser deserialiser initialiser setter getter))

    ((macrotime)
     (make-attribute-type name column-type #f #f #f #f #f))
    
    (else
      (assert #f (conc "Unrecognised phase " phase)))))


;; Attribute Specs
;; This is allocated when an object type declares a field that is an attribute.

(define-record attribute-spec
	       field-idx
	       type
	       name
	       required?
	       constant?
	       key-part)


;; Property Types
;; This is allocated when define-property is used.

(define-record property-type
	       name
	       table
	       key-specs
	       attribute-specs)


(define-record-printer (property-type pt port)
  (fprintf port "#<gromit-property-type ~S ~S ~S>"
	   (property-type-name pt)
	   (map attribute-spec-name (property-type-key-specs pt))
	   (map attribute-spec-name (property-type-attribute-specs pt))))


; Creates a new type of property.
;
; Fields musy be attributes; they can't be other properties.
;
(define (make-property-type* name table fields)
  (let* ((attribute-specs
	   (map
	     (match-lambda*
	       (((name type . flags) field-idx)
		(assert (symbol? name)         (conc "Name for a property field must be a symbol. We got " name "!"))

		(cond
		  ((attribute-type? type)

		   (let ((spec (make-attribute-spec
				 field-idx
				 type
				 name
				 (not (null? (option! 'required flags)))
				 (not (null? (option! 'constant flags)))
				 (let ((o (option! 'key flags)))
				   (cond
				     ((null? o) #f)
				     ((and
					(= 1 (length o))
					(list? (car o))
					(= 2 (length (car o)))
					(integer? (cadar o))
					(exact? (cadar o)))
				      (cadar o))
				     (else
				       (assert #f (conc "Unrecognised key options for attribute " name ": " o))))))))

		     (assert (null? flags) (conc "Unrecognised options for attribute " name ": " flags))

		     spec))

		  (else
		    (assert #f (conc "Type for a property field must be a gromit-attribute-type. We got " type "!"))))))

	     fields
	     (iota (length fields))))

	 (keys
	   (sort ; Put the keys in index order as specified by the (key <n>) flag.
	     (fold ; Find the attribue specs that are keys
	       (lambda (spec lst)
		 (if (attribute-spec-key-part spec)
		   (cons spec lst)
		   lst))
	       '()
	       attribute-specs)
	     (lambda (a b)
	       (<
		 (attribute-spec-key-part a)
		 (attribute-spec-key-part b)))))

	 (attribute-names ; Extract the names of the attributes and sort them.
	   (sort
	     (map
	       attribute-spec-name
	       attribute-specs)
	     (lambda (a b)
	       (string<?
		 (symbol->string a)
		 (symbol->string b))))))

    ; Check that the key parts are specified monotonically, starting from 0.
    (assert (equal?
	      (iota (length keys))
	      (map attribute-spec-key-part keys))
	    (conc "Key parts for Property Type " name " must be in order, starting from 0. We got "
		  (map (lambda (spec)
			 (conc (attribute-spec-name spec) ":" (attribute-spec-key-part spec)))
		       keys)))

    ; Check that each attribute name only appears once.
    (if (not (null? attribute-names))
      (fold
	(lambda (attribute-name seed)
	  (assert
	    (not (eqv? attribute-name seed))
	    (conc "Attribute name " attribute-name " cannot be used more than once in definition for property type " name "!"))
	  attribute-name)
	(car attribute-names)
	(cdr attribute-names)))

    (make-property-type
      name
      table
      keys
      attribute-specs)))


;; Property Specs
;; This is allocated when an object type declares a field that is a property.

(define-record property-spec
	       field-idx
	       type
	       name)


;; Object Types
;; This is allocated when define-object is used.

(define-record object-type
	       name
	       table
	       metadata-type
	       attribute-specs
	       property-specs
	       field-names)


(define (field-spec-name spec)
  ((cond
     ((attribute-spec? spec) attribute-spec-name)
     ((property-spec?  spec) property-spec-name))
   spec))


(define-record-printer (object-type ot port)
  (fprintf port "#<gromit-object-type ~S ~S ~S>"
	   (object-type-name ot)
	   (object-type-table ot)
	   (object-type-field-names ot)))


; Creates a new type of object.
;
; Fields can be either attributes or properties.
; Each property type is allowed no more than once for any of the fields. This
; is for two reasons.
;   1) We don't store enough metadata in the property tables to work out which
;      properties would belong to which field in the object.
;   2) It's not a very "relational" model. Users can get what they want by
;      adding a key to the Property Type.
;
(define (make-object-type* name table metadata-type fields)

  (define (make-counter)
    (let ((c 0))
      (lambda ()
	(let ((r c))
	  (set! c (add1 r))
	  r))))

  (let* ((next-attribute-idx (make-counter))
	 (next-property-idx  (make-counter))

	 (field-specs     ; Turn the list of fields into some field-specs.
	   (map
	     (match-lambda
	       ((name type . flags)
		(assert (symbol? name)         (conc "Name for an object field must be a symbol. We got " name "!"))

		(cond
		  ((attribute-type? type)

		   (let ((spec (make-attribute-spec
				 (next-attribute-idx)
				 type
				 name
				 (not (null? (option! 'required flags)))
				 (not (null? (option! 'constant flags)))
				 #f)))

		     (assert (null? flags) (conc "Unrecognised options for attribute " name ": " flags))

		     spec))

		  ((property-type? type)

		   (let ((spec (make-property-spec
				 (next-property-idx)
				 type
				 name)))

		     (assert (null? flags) (conc "Unrecognised options for property " name ": " flags))

		     spec))

		  (else
		    (assert #f (conc "Type for an object field must be a gromit-attribute-type or a gromit-property-type. We got " type "!"))))))

	     fields))

	 (attribute-specs ; Extract the attribute-specs.
	   (reverse
	     (fold
	       (lambda (spec lst)
		 (if (attribute-spec? spec)
		   (cons spec lst)
		   lst))
	       '()
	       field-specs)))

	 (property-specs  ; Extract the property-specs. 
	   (reverse
	     (fold
	       (lambda (spec lst)
		 (if (property-spec? spec)
		   (cons spec lst)
		   lst))
	       '()
	       field-specs)))

	 (property-specs* ; Order the property-specs based on type.
	   (sort
	     property-specs
	     (lambda (a b)
	       (string<?
		 (symbol->string (property-type-name (property-spec-type a)))
		 (symbol->string (property-type-name (property-spec-type b)))))))

	 (field-names     ; Extract the names of the fields and sort them.
	   (sort
	     (map field-spec-name field-specs)
	     (lambda (a b)
	     (string<?
	       (symbol->string a)
	       (symbol->string b))))))

    ; Check that the metadata-type doesn't have any keys
    (assert (null? (property-type-key-specs metadata-type)) (conc "Metadata Type must not have any key parts but we got " (property-type-name metadata-type) " with " (property-type-key-specs metadata-type "!")))

    ; Check that each proerty-type only appears once.
    (if (not (null? property-specs*))
      (fold
	(lambda (spec seed)
	  (assert
	    (not (eqv? (property-spec-type spec) (property-spec-type seed)))
	    (conc "Property Type " (property-spec-type spec) " cannot be used more than once in definition for object type " name "!"))
	  spec)
	(car property-specs*)
	(cdr property-specs*)))

    ; Check that each field name only appears once.
    (if (not (null? field-names))
      (fold
	(lambda (field-name seed)
	  (assert
	    (not (eqv? field-name seed))
	    (conc "Field name " field-name " cannot be used more than once in definition for object type " name "!"))
	  field-name)
	(car field-names)
	(cdr field-names)))

    (make-object-type
      name
      table
      metadata-type
      attribute-specs
      property-specs
      field-names)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
)

