;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; gromit-records - The runtime, in-memory datastructures.
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
;;; Andy Bennett <andyjpb@register-dynamics.co.uk>, 2019/07
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(module gromit-records
	(;; Macros

	 ;; Helpers
	 no-value
	 not-read

	 ;; Properties
	 make-property-predicate
	 (syntax: define-property-accessors+modifiers make-attribute-accessor+modifier)
	 (syntax: define-default-property-constructor make-property)
	 (syntax: define-property-constructor
		  no-value
		  make-property-list)

	 ;; Objects
	 make-object-predicate
	 (syntax: define-object-accessors+modifiers make-attribute-accessor+modifier make-property-list-accessor+modifier)
	 (syntax: define-default-object-constructor make-object)
	 (syntax: define-object-constructor
		  no-value
		  make-property-list
		  make-attribute-value)
	 define-object-finder

	 ;; Runtime

	 ; Attributes
	 make-attribute-value
	 attribute-value-read
	 attribute-value-current
	 attribute-value-dirty?

	 ; Property Lists
	 make-property-list
	 property-list-read
	 property-list-current

	 ; Properties
	 make-property
	 property-type
	 property-attributes
	 property-allocated?

	 ; Objects
	 make-object
	 object-id
	 object-metadata
	 object-type
	 object-attributes
	 object-properties
	 object-allocated?
	 )

(import chicken scheme)

; Units - http://api.call-cc.org/doc/chicken/language
(use data-structures srfi-1)

; Eggs - http://wiki.call-cc.org/chicken-projects/egg-index-4.html
(use matchable)
(use-for-syntax matchable)

(use            gromit-lolevel)
(use-for-syntax gromit-lolevel)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Helpers

; A symbol to use as the default value for field, attribute and property
; accessors+modifiers.
; If we get this value then we know that the user did not specify the argument.
; This allows the user to set the field to any (other) valid scheme value.
(define no-value (gensym 'no-value))


; A symbol to use to denote that the data has not yet been read from the
; database. If we get this value then we need to run the appropriate read query
; and try again.
(define not-read (gensym 'not-read))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Run Time Structures

;; Attributes
;; These are allocated when an object or a property is read from the database
;; or when an object or a property is written by the user from CHICKEN code.

(define-record attribute-value
	       (setter read)    ; The serialised value as read from the database.
	       (setter current) ; The deserialised value as returned from the attribute-type deserialiser.
	       (setter dirty?)) ; Has the field been explicitly set by the user or a constructor?


;; Property Types

;; Property Lists
;; This is allocated when an object is read from the database.

; A Property List has two slots. the first slot is for a list of Properties
; read from the database. The second slot is the list of Properties as set by
; the user. These start out pointing to the same list. On save these lists are
; sorted and compared to work out whether any Properties need to be added or
; removed.  Even if the lists are identical, the individual Properties must be
; inspected for dirtiness because they could have been mutated without being
; added or removed from the Property List. A Property that is shared between
; two objects will be spotted because it will have to have been added to one of
; the Property Lists using the accessor+modifier. Therefore, even if the
; Property itself is clean, it will still be added to the correct object.

(define-record property-list
	       (setter read)
	       (setter current))


(define-record-printer (property-list pl port)
  (fprintf port "#<gromit-property-list ~S>"
	   (property-list-current pl)))


;; Properties
;; These are allocated when an object's property list is read from the database
;; or an object's property list is set by the user from CHICKEN code.

(define-record property
	       type
	       attributes
	       (setter allocated?))


(define-record-printer (property o port)
  (fprintf port "#<gromit-property ~S ~S ~S>"
	   (if (property-allocated? o)
	     "1:4"
	     'unallocated)
	   (property-type-name (property-type o))
	   "keys"))


(define (make-property-predicate type)
  (lambda (property)
    (and
      (property? property)
      (eq? (property-type property) type))))


;; Objects
;; These are allocated when an object is read from the database or when a new
;; object is made by the user from CHICKEN code.

(define-record object
	       (setter id)
	       type                 ; An object-type that describes the object
	       metadata
	       attributes           ; Attribute values
	       properties           ; Property lists of each property
	       (setter allocated?)) ; Has the object been written to the database yet?


(define-record-printer (object o port)
  (fprintf port "#<gromit-object ~S ~S ~S>"
	   (if (object-allocated? o)
	     "1:4"
	     'unallocated)
	   (object-type-name (object-type o))
	   "name"))


(define (make-object-predicate type)
  (lambda (object)
    (and
      (object? object)
      (eq? (object-type object) type))))


;; Accessors + Modifiers
;; The runtim for accessing and modifying the attributes and properties of
;; objects and the attributes of properties.

; Gets the allocated? flag from an object or a property.
(define (object-or-property-allocated? object-or-property)
  ((cond
     ((object?   object-or-property) object-allocated?)
     ((property? object-or-property) property-allocated?)
     (else
       (assert #f (conc "object-or-property-allocated? expects an object or a property. We got " object-or-property))))
   object-or-property))


; Gets the attributes from an object or a property.
(define (object-or-property-attributes object-or-property)
  ((cond
     ((object?   object-or-property) object-attributes)
     ((property? object-or-property) property-attributes)
     (else
       (assert #f (conc "object-or-property-attributes? expects an object or a property. We got " object-or-property))))
   object-or-property))


; Makes a procedure of two arguments to get and set attribute of objects or
; properties.
; The first argument is the object to operate on.
; If the second argument is present then the field is set to that value and
; returned, subject to the whims of the setter for the specified type.
; If the second argument is absent then the current value of the field is
; returned, subject to the whims of the getter for the specified type. This may
; return #<unspecified> if the object's constructor did not initialise it.
(define (make-attribute-accessor+modifier name idx spec)
  (let ((setter (attribute-type-setter (attribute-spec-type spec)))
	(getter (attribute-type-getter (attribute-spec-type spec))))
    (lambda (object-or-property #!optional (value no-value))
      (if (eqv? value no-value)
	(begin ; getter
	  (getter (attribute-value-current (vector-ref (object-or-property-attributes object-or-property) idx))))
	(begin ; setter
	  (assert (not
		    (and
		      (attribute-spec-constant?       spec)
		      (object-or-property-allocated?  object-or-property)))
		  (conc name ": attribute field is constant and object or property, " object-or-property ",  has already been created. This field cannot be changed!"))
	  (let ((field-value (vector-ref (object-or-property-attributes object-or-property) idx)))
	    (set! (attribute-value-current field-value) (setter value))
	    (set! (attribute-value-dirty?  field-value) #t)
	    value))))))


; Makes a procedure of two arguments to get and set properties-lists of
; objects. If a getter is called on a property that has not been read from the
; database then the reader, the values are saved and then the get operation
; restarts.
; The first argument is the object to operate on.
; If the second argument is present then it must be a list where each item is a
; property of the correct type. After the list has been stashed inside the
; field, it is returned to the caller.
; If the second argument is absent then the current value of the field is
; returned. It will be a list or possibly #<unspecified> if the object's
; constructor did not initialise it.
(define (make-property-list-accessor+modifier name type idx spec pred reader)
  (letrec ((self
	     (lambda (object #!optional (value no-value))
	       (if (eqv? value no-value)

		 ; getter
		 (let* ((property-list (vector-ref (object-properties object) idx))
			(list-current  (property-list-current property-list)))
		   (if (eq? not-read list-current)

		     (begin ; Read the properties from the database and the restart.

		       (assert (eq? not-read (property-list-read property-list)) (conc "make-property-list-accessor+modifier: list-current was not-read so expecting list-read to be not-read as well. We got " (property-list-read property-list) "!"))

		       (let* ((object-id         (object-id object))
			      (list-read (reader object-id))
			      (_ (assert (not (eq? not-read list-read)) (conc "make-property-list-accessor+modifier: list-current was not-read so was expecting a list after calling reader. We got " list-read "!")))
			      (list-read
				(map
				  (match-lambda
				    ((read-object-id property)

				     (assert (equal? object-id read-object-id) (conc "makke-property-list-accessor+modifier: Expected object-id " object-id " whilst reading property. We got " property "!"))

				     property))
				  list-read)))
			 (set!
			   (property-list-read property-list)
			   list-read)
			 (set!
			   (property-list-current property-list)
			   list-read)
			 (self object value)))

		     (begin ; Return the properties from memory.

		       (assert (list? list-current) (conc "make-property-list-accessor+modifier: getter expected property-list-current to be a list. We got " list-current "!"))

		       list-current)))

		 ; setter
		 (let ((property-list (vector-ref (object-properties object) idx)))

		   (assert (list? value)      (conc name ": property field for object " object " expects a list. We got " value "!"))
		   (assert (every pred value) (conc name ": property field for object " object " expects a list where each member is a " type ". We got " value "!"))

		   (set!
		     (property-list-current property-list)
		     value)
		   value)))))
    self))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Compile Time Macros
;;; These are the helpers for the language that the user uses to specify their
;;; objects and properties.
;;; define-object-constructor and define-property-constructor are part of the
;;; user facing API.

;; Accessors + Modifiers

; Makes a procedure that accesses an attribute for an object or a procedure.
(define-for-syntax (attribute-accessor+modifier-maker TYPE* specs-ref)
  (lambda (field-spec idx)
    (let* ((FIELD  (field-spec-name field-spec))
	   (FIELD* (strip-syntax    FIELD))
	   (NAME*  (symbol-append   TYPE* '- FIELD*)))
      `(define ,NAME*
	 (let ((spec (list-ref (,specs-ref ,TYPE*) ,idx)))
	   (make-attribute-accessor+modifier ',NAME* ,idx spec))))))


; Makes a procedure that accesses a property list for an object.
(define-for-syntax (property-list-accessor+modifier-maker TYPE*)
  (lambda (field-spec idx)
    (let* ((FIELD               (field-spec-name    field-spec))
	   (FIELD*              (strip-syntax       FIELD))
	   (NAME*               (symbol-append      TYPE* '- FIELD*))
	   (property-type       (property-spec-type field-spec))
	   (PROPERTY-TYPE-NAME  (property-type-name property-type))
	   (PROPERTY-TYPE-NAME* (strip-syntax       PROPERTY-TYPE-NAME))
	   (PRED                (symbol-append      PROPERTY-TYPE-NAME* '?))
	   (READER              (symbol-append      'read-               PROPERTY-TYPE-NAME*)))
      `(define ,NAME*
	 (let ((spec (list-ref (object-type-property-specs ,TYPE*) ,idx)))
	   (make-property-list-accessor+modifier ',NAME* ',PROPERTY-TYPE-NAME* ,idx spec ,PRED ,READER))))))

	 

;; Properties

; Defines an accessor+modifier for each field.
(define-syntax define-property-accessors+modifiers
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-property-accessor+modifiers TYPE rtd)
	 (let* ((TYPE*           (strip-syntax TYPE))
		(attribute-specs (property-type-attribute-specs rtd))
		(n-attributes    (length attribute-specs)))
	   `(begin
	      ,@(map (attribute-accessor+modifier-maker TYPE* 'property-type-attribute-specs) attribute-specs (iota n-attributes)))))))))


; Defines a property constructor that creates an empty new property structure
; and passes it to the user supplied procedure for initialisation.
(define-syntax define-property-constructor
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx

	((define-property-constructor CONSTRUCTOR-NAME TYPE syntax-type PROC)
	 (let* ((attribute-specs (property-type-attribute-specs syntax-type)))
	   `(define ,CONSTRUCTOR-NAME
	      (let ((proc ,PROC))
		(lambda args
		  (let ((new-property
			  (make-property
			    ,TYPE   ; type
			    (vector ,@(map (lambda _ `(make-attribute-value no-value (void) #f)) attribute-specs)) ; attributes
			    #f)))  ; allocated?
		    (apply proc new-property args)
		    new-property))))))

	; Find and resolve the syntax-TYPE object because we weren't passed it explicitly.
	((define-property-constructor CONSTRUCTOR-NAME TYPE PROC)
	 `(define-property-constructor ,CONSTRUCTOR-NAME ,TYPE ,(##sys#slot (field-type->syntax-type* TYPE) 0) ,PROC))))))


; Makes the default property constructor that has field names in its signature.
(define-syntax define-default-property-constructor
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-default-property-constructor NAME TYPE syntax-type)
	 (let* ((attribute-names (map attribute-spec-name (property-type-attribute-specs syntax-type)))
		(NAME%           (symbol-append '% NAME)) ; Put the % at the beginning rather than the end so that we don't collide with things that are names TYPE%.
		(TYPE*           (strip-syntax TYPE)))
	   `(begin
	      (define-property-constructor
		,NAME%
		,TYPE*
		,syntax-type
		(lambda (property ,@attribute-names)
		  ; All attributes must always be supplied in the default
		  ; constructor.
		  ,@(begin 
		      (map
			(lambda (field-name)
			  `(,(symbol-append TYPE* '- (strip-syntax field-name)) property ,field-name))
			attribute-names))))
	      (define ,NAME
		(lambda (,@attribute-names)
		  (,NAME% ,@attribute-names))))))))))


;; Objects

; Defines an accessor+modifier for each field.
(define-syntax define-object-accessors+modifiers
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-object-accessor+modifiers TYPE rtd)
	 (let* ((TYPE*           (strip-syntax TYPE))
		(attribute-specs (object-type-attribute-specs rtd))
		(n-attributes    (length attribute-specs))
		(property-specs  (object-type-property-specs rtd))
		(n-properties    (length property-specs)))
	   `(begin
	      ,@(map (attribute-accessor+modifier-maker     TYPE* 'object-type-attribute-specs) attribute-specs (iota n-attributes))
	      ,@(map (property-list-accessor+modifier-maker TYPE*)                              property-specs  (iota n-properties)))))))))


; Defines an object constructor that creates an empty new object structure and
; passes it to the user supplied procedure for initialisation.
(define-syntax define-object-constructor
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx

	((define-object-constructor CONSTRUCTOR-NAME TYPE syntax-type PROC)
	 (let* ((attribute-specs (object-type-attribute-specs syntax-type))
		(property-specs  (object-type-property-specs  syntax-type))
		(metadata-type   (object-type-metadata-type  syntax-type))
		(metadata-specs  (property-type-attribute-specs metadata-type)))
	   `(define ,CONSTRUCTOR-NAME
	      (let ((proc ,PROC))
		(lambda args
		  (let ((new-object
			  (make-object
			    #f                         ; id
			    ,TYPE                      ; type
			    (make-property             ; metadata
			      ,(property-type-name metadata-type) ; The symbol that resolves to the property-type.
			      (vector ,@(map (lambda _ `(make-attribute-value no-value (void) #f)) metadata-specs))
			      #f)
			    (vector ,@(map (lambda _ `(make-attribute-value no-value (void) #f)) attribute-specs))
			    (vector ,@(map (lambda _ `(make-property-list   '()      '()))       property-specs))
			    #f)))                      ; allocated?
		    (apply proc new-object args)
		    new-object))))))

	; Find and resolve the syntax-TYPE object because we weren't passed it explicitly.
	((define-object-constructor CONSTRUCTOR-NAME TYPE PROC)
	 `(define-object-constructor ,CONSTRUCTOR-NAME ,TYPE ,(##sys#slot (field-type->syntax-type* TYPE) 0) ,PROC))))))


; Makes the default object constructor that has field names in its signature.
(define-syntax define-default-object-constructor
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-default-object-constructor NAME TYPE syntax-type)
	 (let* ((attribute-names (map attribute-spec-name (object-type-attribute-specs syntax-type)))
		(property-names  (map property-spec-name (object-type-property-specs syntax-type)))
		(NAME%           (symbol-append '% NAME))  ; Put the % at the beginning rather than the end so that we don't collide with things that are names TYPE%.
		(TYPE*           (strip-syntax TYPE)))
	   `(begin
	      (define-object-constructor
		,NAME%
		,TYPE*
		,syntax-type
		(lambda (object ,@attribute-names ,@property-names)
		  ; All attributes must always be supplied in the default
		  ; constructor.
		  ,@(begin 
		      (map
			(lambda (field-name)
			  `(,(symbol-append TYPE* '- (strip-syntax field-name)) object ,field-name))
			attribute-names))
		  ; The default property value passed via #!key args is #f. We
		  ; don't set it to '(), which is what we really want, because
		  ; it messes up our pretty printer:
		  ; #<procedure (make-account120 pre-genesis onboarding #!key password)>
		  ; vs
		  ; #<procedure (make-account120 pre-genesis onboarding #!key (password (quote171 ())))>
		  ; ...so here we generate some code to conditionally set the
		  ; property. We can get away with this without defaulting the
		  ; argument to no-value because properties must always be set
		  ; to a list of values.
		  ,@(begin
		      (map
			(lambda (field-name)
			  `(if ,field-name
			     (,(symbol-append TYPE* '- (strip-syntax field-name)) object ,field-name)))
			property-names))))
	      (define ,NAME
		(lambda (,@attribute-names #!key ,@property-names)
		  (,NAME% ,@attribute-names ,@property-names))))))))))


(define-syntax define-object-finder
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-object-finder SIGNATURE TYPE CONDITIONS)
	 `(define-object-finder/sqlite ,SIGNATURE (fields->list runtime ,(cdr SIGNATURE)) ,TYPE ,CONDITIONS))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
)

