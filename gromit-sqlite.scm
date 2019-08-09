;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; gromit-sqlite - Persist to and retrieve from an SQLite database.
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

(module gromit-sqlite
	(;; Runtime
	 current-gromit-db
	 open-gromit-database

	 ;; Transactions
	 with-gromit-read-transaction
	 with-gromit-write-transaction
	 with-gromit-nested-transaction
	 with-gromit-nested-write-transaction

	 ; Properties
	 define-property-create-table/sqlite
	 (syntax: define-property-allocator/sqlite
		  ensure-writing-object
		  ensure-write-transaction
		  make-table-mutator
		  ensure-attributes-initialised!
		  ensure-attributes-reread!)
	 (syntax: define-property-updater/sqlite
		  ensure-writing-object
		  ensure-write-transaction
		  make-table-mutator)
	 (syntax: define-property-reader/sqlite
		  make-table-reader
		  ensure-attributes-reread!)

	 ; Objects
	 define-object-create-table/sqlite
	 (syntax: define-object-allocator/sqlite
		  writing-object?
		  make-table-mutator)
	 (syntax: define-object-updater/sqlite
		  writing-object?
		  make-table-mutator)
	 (syntax: define-object-reader/sqlite
		  make-table-reader
		  ensure-attributes-reread!)
	 (syntax: define-object-finder/sqlite
		  make-table-reader)
	 )

(import chicken scheme)

; Units - http://api.call-cc.org/doc/chicken/language
(use data-structures srfi-1)

; Eggs - http://wiki.call-cc.org/chicken-projects/egg-index-4.html
(use matchable vector-lib)
(use-for-syntax matchable ssql)

(use            gromit-consistency gromit-lolevel gromit-records sql-de-lite)
(use-for-syntax gromit-lolevel)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal State

;; Parameters

; The default database handle to use
(define current-gromit-db (make-parameter #f))


; This parameter is true when an object allocator or updater is running further
; up the call chain.
(define writing-object? (make-parameter #f))


(define (ensure-writing-object)
  (assert (writing-object?)
	  (conc "ensure-writing-object: This operation can only be done if an object is being written!")))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Database Handles

; Returns something suitable for use as (current-gromit-db).
(define (open-gromit-database filename)
  (let ((handle (open-database filename)))
    (set-busy-handler! handle (busy-timeout 10000)) ; 10 second timeout
    handle))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Transactions

(define current-transaction (make-parameter 'none))


(define (ensure-write-transaction)
  (assert (eq? 'write (current-transaction))
	  (conc "ensure-write-transaction: This operation requires a write transaction. We got " (current-transaction))))


(define (with-gromit-read-transaction db thunk)
  (with-deferred-transaction
    db
    (lambda ()

      (assert (eq? 'none (current-transaction))
	      (conc "with-gromit-read-transaction: Can't begin a read transaction because another transaction is already running. We got " (current-transaction)))

      (parameterize ((current-transaction 'read))
	(thunk)))))


(define (with-gromit-write-transaction db thunk)
  (with-immediate-transaction
    db
    (lambda ()

      (assert (eq? 'none (current-transaction))
	      (conc "with-gromit-write-transaction: Can't begin a write transaction because another transaction is already running. We got " (current-transaction)))

      (parameterize ((current-transaction 'write))
	(thunk)))))


(define (with-gromit-nested-transaction db thunk)
  (with-savepoint-transaction
    db
    (lambda ()
      (parameterize ((current-transaction (if (eq? 'none (current-transaction))
					    'read
					    (current-transaction))))
	(thunk)))))


(define (with-gromit-nested-write-transaction db thunk)
  ((case (current-transaction)
     ((none)  with-gromit-write-transaction)
     ((write) with-gromit-nested-transaction)
     (else
       (assert #f (conc "with-gromit-nested-write-transaction: Can't begin a nested write transaction because another transaction is already running. We got " (current-transaction)))))
   db
   thunk))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Internal Helpers

; Makes a procedure for use by the create-* and save-* definitions that runs a
; query that mutates a database table containing the attributes for object,
; attribute or property definitions.
; We are passed in a pregenerated query string and just need to serialise and
; bind all the arguments. The appropriate fields, which are expected to be
; initialised, are bound to the statement in the correct order based on the
; attribute-specs. The query is then executed using sql-de-lite's exec.
;
; query           : A string representing the SQL query to run. query should
;                   contain parameters for node-id and seqno and a number of
;                   attribute fields.
;
; attribute-specs : A list of attribute specs that define the fields that we
;                   want to operate on. The list should be long enough to
;                   provide one spec per field parameter in query.
;
; attributess     : A vector of fields to extract the field data from. The
;                   elements that are actually used will depend on the list of
;                   attribute specs passed in.
;
; The serialiser for each attribute is called. After the table mutation is
; finished and the SQL transaction is closed, we will revisit each attribute
; and call the deserialiser so that the object looks like it was just read from
; the database. This ensures that attributes whose serialisers change the field
; value reflect the latest value for the attribute.
(define (make-table-mutator query table attribute-key-specs attribute-specs)
  (lambda (keys attributes)
    (let ((rows-changed
	    (apply
	      exec
	      (sql (current-gromit-db) query)
	      (append
		keys ; metadata keys
		(map ; attribute keys
		  (lambda (spec)
		    ; We don't call the serialiser here because the stashed
		    ; value we read from the database is never deserialised in
		    ; the first place! This allows us to have types where the
		    ; serialiser generates a new value each time it is called.
		    (attribute-value-read (vector-ref attributes (attribute-spec-field-idx spec))))
		  attribute-key-specs)
		(map ; attributes
		  (lambda (spec)
		    (let* ((value (vector-ref attributes (attribute-spec-field-idx spec)))
			   ; Always serialise the current value, whether or not the
			   ; user actively changed it. This allows for
			   ; attribute-types that update themselves.
			   (serialised-value ((attribute-type-serialiser (attribute-spec-type spec))
					      (attribute-value-current value))))

		      ; Stash the serialised value so that later we can
		      ; attribute-value look like it was just read from the
		      ; database.
		      (set!
			(attribute-value-read value)
			serialised-value)

		      ; Write the seralised value to the database.
		      serialised-value))
		  attribute-specs)))))
      (assert
	(= 1 rows-changed)
	(conc "make-table-mutator for " table " " keys " should have mutated 1 row. We got " rows-changed "!")))))


; Calls the appropriate initialiser for the attribute fields defined by the
; specs and that are not dirty.
; This should only be called by a create-* definition that has already checked
; the object or property has not yet been allocated.
; We call the initialiser for non-dirty fields each time an attempt is made to
; allocate the object or property. If we initialise a field, we mark it as
; dirty. This means that if the allocation fails, the field will not be
; initialised again.
(define (ensure-attributes-initialised! specs attributes)
  (for-each
    (lambda (spec)
      (let* ((attribute-type (attribute-spec-type spec))
	     (value          (vector-ref attributes (attribute-spec-field-idx spec))))
	(if (not (attribute-value-dirty? value))
	  (begin
	    (set!
	      (attribute-value-current value)
	      ((attribute-type-initialiser attribute-type)))
	    (set!
	      (attribute-value-dirty? value)
	      #t)))))
    specs))


; Ensures that all attribute-value-current values are up-to-date after
; attribute-value serialisers have been called.
(define (ensure-attributes-reread! specs attributes)
  (for-each
    (lambda (spec)
      (let* ((attribute-type (attribute-spec-type spec))
	     (value          (vector-ref attributes (attribute-spec-field-idx spec))))
	; Only reread the attribute if there is something to reread. In the
	; case where an allocator fails halfway thru' (for example due to a
	; UNIQUE constraint violation) some of the fields may still not have
	; been initialised. In that case we want to keep whatever the user put
	; in attribute-value-current. This is safe because constructors are the
	; only things that put no-value into attribute-value-read.
	(if (not (eq? no-value (attribute-value-read value)))
	  (begin
	    (set! ; Some serialisers can update the object.
	      (attribute-value-current value)
	      ((attribute-type-deserialiser (attribute-spec-type spec))
	       (attribute-value-read value)))
	    (set!
	      (attribute-value-dirty? value)
	      #f)))))
    specs))


; Makes a procedure for use by the read-* and find-* definitions that runs a
; query that reads data from a database table containing the attributes for
; object, attribute or property definitions.
; Returns a list of lists containing the attribute-value vectors for each
; object found.
; We are passed in a pregenerated query string and just need to serialise and
; bind all the arguments and then deserailase the result. The appropriate
; fields, which are expected to be initialised, are bound to the statement in
; the correct order based on the attribute-specs. The query is then executed
; using sql-de-lite's query.
;
; query-string    : A string representing the SQL query to run. query should
;                   contain parameters for some kind of WHERE clause. For
;                   example, node-id and seqno or a number of attribute fields.
;
; attribute-specs : A list of lists of attribute specs that define the fields
;                   that we want to retrieve. There should be enough
;                   attribute-specs to consume all the columns in in the result
;                   set after the key columns have been consumed. Columns will
;                   be bound to attribute-specs in order.
;
; The serialised value for each attribute is stashed in the read part of the
; attribute-value. After the table read is finished and the SQL transaction is
; closed, we will revisit each attribute and call the deserialisers.
(define (make-table-reader query-string n-keys . attribute-specs)
  (let ((attribute-strides
	  (map
	    (lambda (specs)
	      (length specs))
	    attribute-specs)))

    (lambda constraints
      (apply
	query
	(map-rows
	  (lambda (row)
	    (let* ((key-values (take row n-keys))
		   (key-values (list->vector key-values))
		   (row        (drop row n-keys))
		   (result
		     (fold
		       (match-lambda*
			 ((attribute-specs n-attributes (row result))
			  (let* ((attributes (take row n-attributes))
				 (row        (drop row n-attributes))
				 (attributes (list->vector (map (lambda (attribute)
								  (make-attribute-value
								    attribute ; read
								    no-value  ; current - this will be populated later
								    #f))      ; dirty?
								attributes))))
			    `(,row ,(cons attributes result)))))
		       `(,row (,key-values)) ; '(unprocessed-row . results)
		       attribute-specs
		       attribute-strides)))
	      (let ((row    (first result))
		    (result (reverse (second result))))
		(assert (null? row) (conc "make-table-reader: Expected map-rows to consume all the columns. We were left with " row "!"))
		result))))
	(sql (current-gromit-db) query-string)
	constraints)))) ; metadata keys




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Compile Time Query Generation

; Takes a number and returns a symbol for use as a parameter in an ssql query.
(define-for-syntax (n->parameter n)
  (string->symbol
    (string-append
      "?"
      (number->string n))))


; Takes a lists of column names and turns them into a list of sequentially
; numbered SQL Parameters for use in an ssql quey.
(define-for-syntax (columns->parameters start . cols)
  (map
    n->parameter
    (iota
      (apply + (map length cols))
      start)))


; Generates a CREATE TABLE SQL query.
; keys is a list of lists thus: '((node-id INTEGER) (seqno INTEGER) ...)
(define-for-syntax (CREATE-TABLE table-name keys attribute-key-specs attribute-specs)
  (ssql->sql
    #f
    `(create-table ,table-name
		   (columns
		     ,@(map ; keys
			 (match-lambda
			   ((name type)
			    `(,name ,type NOT NULL)))
			 keys)
		     ,@(map ; attributes
			 (lambda (spec)
			   `(,(attribute-spec-name spec)
			      ,(attribute-type-column-type (attribute-spec-type spec))
			      ,@(if (attribute-spec-required? spec)
				  '(NOT NULL)
				  '())))
			 attribute-specs))
		   (primary-key
		     ,@(map ; keys
			 car
			 keys)
		     ,@(map ; attribute keys
			 attribute-spec-name
			 attribute-key-specs)))))


; Generates a parameterised SQL query.
(define-for-syntax (INSERT table-name keys attribute-specs)
  (ssql->sql
    #f
    `(insert (into ,table-name)
	     (columns
	       ,@keys ; keys
	       ,@(map ; attributes
		   attribute-spec-name
		   attribute-specs))
	     (values
	       #(,@(columns->parameters 1 keys attribute-specs))))))


; Generates a parameterised SQL query.
; The key and attribute-key parameters for the WHERE clase come first and then
; a parameter for each column in the table, including the attribute keys but
; excluding the object identifier keys.
(define-for-syntax (UPDATE table-name keys attribute-key-specs attribute-specs)
  (let ((total-keys (+ (length keys) (length attribute-key-specs))))
    (ssql->sql
      #f
      `(update ,table-name
	       (set
		 ,@(map ; attributes
		     (lambda (spec parameter)
		       `(,(attribute-spec-name spec) ,parameter))
		     attribute-specs
		     (columns->parameters (add1 total-keys) attribute-specs)))
	       (where
		 (and
		   ,@(map ; keys
		       (lambda (key parameter)
			 `(= (col ,table-name ,key) ,parameter))
		       keys
		       (columns->parameters 1 keys))
		   ,@(map ; attribute-keys
		       (lambda (spec parameter)
			 `(= (col ,table-name ,(attribute-spec-name spec)) ,parameter))
		       attribute-specs
		       (columns->parameters (add1 (length keys)) attribute-key-specs))))))))


; Generates a parameterised SQL query.
; key-table-name : The table name to use for the keys.
; keys           : The list of keys that scopes the WHERE clause and that are
;                  used to join the tables.
; table-specs    : A list of lists containing table names and attribute-specs
;                  for that table. Joining a table to itself is not currently
;                  supported because we don't use the "as" clause in the join
;                  grammar. Therefore, each table may only appear once in the
;                  list of table-specs.
;                  '((objects object-specs...) (attribute attribute-specs...))
(define-for-syntax (SELECT distinct? key-table-name keys where . table-specs)

  (define (on table-a table-b)
    `(on
       (and
	 ,@(map
	     (lambda (key)
	       `(= (col ,table-a ,key) (col ,table-b ,key))) ; TODO: allow some foreign key specifications so that we can join the edges table.
	     keys))))

  (let* ((select-table-specs ; Filter the table specs for ones that contain attribute specs as well as a table name.
	   (reverse
	     (fold
	       (lambda (spec seed)
		 (match spec
		   ((table-name attribute-specs)
		    (if (null? attribute-specs)
		      seed
		      (cons spec seed)))))
	       '()
	       table-specs)))
	 (join-table-specs   ; We need to join to all the tables, whether or not we end up selecting columns from them.
	   table-specs)
	 (query              ; Build the query, except the WHERE clause.
	   `(select
	      ,@(if distinct? '(DISTINCT) '())
	      (columns
		; Column names for each key part.
		(col ,key-table-name ,@keys)
		,@(map ; Column names for each attribute-spec of each table.
		    (match-lambda
		      ((table-name attribute-specs)
		       (let ((attribute-names (map attribute-spec-name attribute-specs)))
			 `(col ,table-name ,@attribute-names))))
		    select-table-specs))
	      ,(if (null? join-table-specs)
		 `(from ,key-table-name)
		 `(from ; Generate the JOINS if necessary
		    ,(let* ((first-table-name (first (car join-table-specs)))
			    (rest-table-names (map first (cdr join-table-specs))))
		       (second
			 (fold 
			   (match-lambda*
			     ((table-name (prev-table-name clause))
			      `(,table-name ; next prev-table-name
				 (join     ; next clause
				   inner ,table-name
				   ,clause
				   ,(on prev-table-name table-name)))))
			   `(,first-table-name ,first-table-name) ; '(table-name clause)
			   rest-table-names)))))))

	 (query ; Add a WHERE clause
	   (cond
	     (where                    ; Use the supplied WHERE clause.
	       (append query `((where ,where))))
	     ((null? join-table-specs) ; No WHERE clause.
	      query)
	     (else                     ; Add the WHERE clause based on the keys.
	       (scope-table
		 key-table-name ; TODO: refactor so that each key has its own table name
		 `(and
		    ,@(map ; keys
			(lambda (key parameter)
			  `(= (col ,key-table-name ,key) ,parameter))
			keys
			(columns->parameters 1 keys)))
	       query)))))

    (ssql->sql
      #f
      query)))


; Takes a condition clause containing sublists that represent attributes,
; property attributes, object metadata attributes and references to arguments
; and converts it into an ssql where clause with positional parameters and a
; list of table names that must appear in the query where the where clause is
; used.
; In the condition clause, the following constructs have special meanings:
;   (@ attribute)          : refers to an object's attribute.
;   (@ property attribute) : refers to the attribute belonging to an object's
;                            property.
;   (@@ attribute)         : refers to an object's metadata attribute.
;   ,symbol                : refers to an argument. The symbol must appear in
;                            the arguments argument.
(define-for-syntax (WHERE syntax-type arguments conditions)

  (let ((table-specs '()))

    ; Add a table to the list of table-specs.
    ; Make the alist value '() so that it forms a valid table-spec for SELECT.
    (define (add-table! table-name)
      (set!
	table-specs
	(alist-update! table-name '(()) table-specs)))

    ; Go through the sexpr, substituting the special constructs for their ssql
    ; equivalents.
    (define (reconstruct argument-map sexpr)
      (reverse
	(fold
	  (lambda (sexpr seed)
	    (cons
	      (match sexpr

		; object attribute
		(('@ attribute)
		 (let ((table-name (object-type-table syntax-type)))
		   (add-table! table-name)
		   `(col ,table-name ,attribute)))

		; property attribute
		(('@ property attribute)
		 (let* ((property-spec
			  (find
			    (lambda (spec)
			      (eqv?
				(property-spec-name spec)
				property))
			    (object-type-property-specs syntax-type)))
			(_ (assert property-spec (conc "WHERE: could not find property with name " property " for object type " (object-type-name syntax-type))))
			(table-name (property-type-table (property-spec-type property-spec))))
		   (add-table! table-name)
		   `(col ,table-name ,attribute)))

		; metadata attribute
		(('@@ attribute)
		 (let* ((metadata-type (object-type-metadata-type syntax-type))
			(table-name    (property-type-table       metadata-type)))
		   (add-table! table-name)
		   `(col ,table-name ,attribute)))

		; argument
		(('unquote argument)

		 (let ((argument-map (assq argument argument-map)))
		   (if argument-map
		     (second argument-map)
		     sexpr))) ; FIXME: Unquoting of non-arguments is not supported.

		; sub-condition
		((anything ...)
		 (reconstruct argument-map sexpr))

		; standalone symbol
		(symbol
		  sexpr))
	      seed))
	  '()
	  sexpr)))


    (values

      (reconstruct     ; Return the conditions for the WHERE clause.

	; Create a mapping between argument names and ssql positional parameters.
	(zip
	  arguments
	  (columns->parameters 1 arguments))

	conditions)

	table-specs))) ; Return the table-specs of the tables involved in the WHERE clause.



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Compile Time Macros
;;; These are the helpers for the language that the user uses to specify their
;;; objects and properties.
;;; We provide macros that build the relevant create-, save-, read- and find-
;;; procedures for objects and properties.

;; Properties

; Defines create-table-TYPE which is a string representing a CREATE TABLE SQL
; query.
(define-syntax define-property-create-table/sqlite
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-property-create-table/sqlite NAME TYPE syntax-type)
	 `(define ,NAME
	    ,(CREATE-TABLE
	       (property-type-table syntax-type)
	       '((node-id INTEGER) (seqno INTEGER))
	       (property-type-key-specs syntax-type)
	       (property-type-attribute-specs syntax-type))))))))


; Defines a %create-TYPE procedure.
; Do the query generation for the property table at compile time. 
; The procedure that we generate ensures that it is called from within an
; object allocator.
(define-syntax define-property-allocator/sqlite
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-property-allocator/sqlite NAME TYPE PREDICATE syntax-type)
	 (let* ((property-table-name  (property-type-table syntax-type))
		(property-table-query (INSERT property-table-name
					      `(node-id seqno)
					      (property-type-attribute-specs syntax-type))))
	   `(define ,NAME
	      (let* ((type                ,TYPE)
		     (table               (property-type-table type))
		     (attribute-specs     (property-type-attribute-specs type))
		     (attribute-allocator (make-table-mutator ,property-table-query table '() attribute-specs)))
		(lambda (node-id seqno property)

		  (assert (,PREDICATE property)                (conc ,NAME ": expected a " ',TYPE " property. We got " property "!"))
		  (assert (not (property-allocated? property)) (conc ,NAME ": cannot create already allocated property " property))

		  (ensure-writing-object)

		  (let ((attributes (property-attributes property)))

		    (ensure-write-transaction)

		    (with-gromit-nested-transaction
		      (current-gromit-db)
		      (lambda ()

			; We end up calling user code such as the attribute-type
			; procedures which may lock up whilst trying to allocate
			; sequence numbers and things!

			(attribute-allocator `(,node-id ,seqno) attributes)))

		    ; We don't set the allocated? flag here because if the
		    ; transaction later fails we have no way to reset it. The
		    ; allocated? flags are all set in the object allocator
		    ; after the transaction commits successfully.

		    property))))))))))


; Defines a %save-TYPE! procedure.
; Do the query generation for the property table at compile time. 
; The procedure that we generate ensures that it is called from within an
; object updater.
(define-syntax define-property-updater/sqlite
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-property-updater/sqlite NAME TYPE PREDICATE syntax-type)
	 (let* ((property-table-name  (property-type-table syntax-type))
		(property-table-query (UPDATE property-table-name
					      `(node-id seqno)
					      (property-type-key-specs       syntax-type)
					      (property-type-attribute-specs syntax-type))))
	   `(define ,NAME
	      (let* ((type ,TYPE)
		     (table             (property-type-table type))
		     (key-specs         (property-type-key-specs       type))
		     (attribute-specs   (property-type-attribute-specs type))
		     (attribute-updater (make-table-mutator ,property-table-query table key-specs attribute-specs)))
		(lambda (node-id seqno property)

		  (assert (,PREDICATE property)          (conc ,NAME ": expected a " ',TYPE " property. We got " property "!"))
		  (assert (property-allocated? property) (conc ,NAME ": cannot save unallocated property " property))

		  (ensure-writing-object)

		  (ensure-write-transaction)

		  (with-gromit-nested-transaction
		    (current-gromit-db)
		    (lambda ()

		      (attribute-updater `(,node-id ,seqno) (property-attributes property))))

		  property)))))))))


; Defines a %read-TYPE! procedure.
; Do the query generation for the property table at compile time. 
; Returns a list of lists containing a vector representing an object-id and an
; attributes that must be poked into the read slot of a property-list.
(define-syntax define-property-reader/sqlite
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-property-reader/sqlite NAME TYPE syntax-type)
	 (let* ((property-table-name  (property-type-table           syntax-type))
		(attribute-specs      (property-type-attribute-specs syntax-type))
		(read-query           (SELECT #f property-table-name '(node-id seqno) #f
					      `(,property-table-name  ,attribute-specs))))
	   `(define ,NAME
	      (let* ((type            ,TYPE)
		     (attribute-specs (property-type-attribute-specs   type))
		     (reader          (make-table-reader ,read-query 2
							 attribute-specs)))
		(lambda (object-id)

		  (let* ((node-id    (object-id-node-id object-id))
			 (seqno      (object-id-seqno   object-id))
			 (all-values (reader node-id seqno)))
		    (map
		      (lambda (property)
			(let* ((object-id        (first  property))
			       (attribute-values (second property)))

			  (ensure-attributes-reread! attribute-specs attribute-values)

			  `(,object-id
			     ,(make-property
				type             ; type
				attribute-values ; attributes
				#t))))           ; allocated?
		      all-values)))))))))))


;; Objects

; Defines create-table-TYPE which is a string representing a CREATE TABLE SQL
; query.
(define-syntax define-object-create-table/sqlite
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-object-create-table/sqlite NAME TYPE syntax-type)
	 `(define ,NAME
	    ,(CREATE-TABLE
	       (object-type-table syntax-type)
	       '((node-id INTEGER) (seqno INTEGER))
	       '()
	       (object-type-attribute-specs syntax-type))))))))


; Defines a create-TYPE procedure.
; Do the query generation for the attribute table at compile time. Call out to
; the property table allocators as needed based on the object schema.
(define-syntax define-object-allocator/sqlite
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-object-allocator/sqlite NAME TYPE PREDICATE syntax-type)
	 (let* ((TYPE*                 (strip-syntax TYPE))
		(metadata-type         (object-type-metadata-type syntax-type))
		(metadata-table-name   (property-type-table metadata-type))
		(metadata-table-query  (INSERT metadata-table-name
					       `(node-id seqno type)
					       (property-type-attribute-specs metadata-type)))
		(attribute-table-name  (object-type-table syntax-type))
		(attribute-table-query (INSERT attribute-table-name
					       `(node-id seqno)
					       (object-type-attribute-specs syntax-type))))
	   `(define ,NAME
	      (let* ((type                ,TYPE*)
		     (type-name           (symbol->string (object-type-name type)))
		     (table               (object-type-table type))
		     (metadata-type       (object-type-metadata-type type))
		     (metadata-table      (property-type-table metadata-type))
		     (metadata-specs      (property-type-attribute-specs metadata-type))
		     (metadata-allocator  (make-table-mutator ,metadata-table-query  metadata-table '() metadata-specs))
		     (attribute-specs     (object-type-attribute-specs type))
		     (attribute-allocator (make-table-mutator ,attribute-table-query table          '() attribute-specs)))
		(lambda (object)

		  (parameterize ((writing-object? #t))

		    (assert (,PREDICATE object)              (conc ,NAME ": expected a " ',TYPE " object. We got " object "!"))
		    (assert (not (object-allocated? object)) (conc ,NAME ": cannot create already allocated object " object))

		    (let ((metadata   (property-attributes (object-metadata object)))
			  (attributes (object-attributes   object))
			  (properties (object-properties   object))
			  (node-id    gromit-node-id)
			  (seqno      (gromit-next-seqno)))

		      (ensure-attributes-initialised! metadata-specs  metadata)
		      (ensure-attributes-initialised! attribute-specs attributes)

		      ; Call ensure-attributes-initialised! for each property.
		      ; We do this outside the transaction to keep the
		      ; transaction as short as possible.

		      ,@(map

			  (lambda (spec)
			    (let* ((type            (property-spec-type spec))
				   (PROPERTY        (property-type-name type))
				   (FIELD-NAME      (property-spec-name spec))
				   (TYPE-PROPERTY   (symbol-append TYPE* '- FIELD-NAME)))
			      `(let ((property-list   (,TYPE-PROPERTY object))
				     (attribute-specs (property-type-attribute-specs ,PROPERTY)))

				 (for-each
				   (lambda (property)
				     ; No need to call PROPERTY? because this is enforced at set time.
				     (ensure-attributes-initialised!
				       attribute-specs
				       (property-attributes property)))
				   property-list))))

			  (object-type-property-specs syntax-type))


		      (dynamic-wind
			values     ; before
			(lambda () ; thunk

			  (with-gromit-nested-write-transaction
			    (current-gromit-db)
			    (lambda ()

			      ; We end up calling user code such as the attribute-type
			      ; procedures which may lock up whilst trying to allocate
			      ; sequence numbers and things!

			      (metadata-allocator  `(,node-id ,seqno ,type-name) metadata)
			      (attribute-allocator `(,node-id ,seqno)            attributes)

			      ,@(map

				  (lambda (spec)
				    (let* ((type             (property-spec-type spec))
					   (field-idx        (property-spec-field-idx spec))
					   (PROPERTY         (property-type-name type))
					   (%create-PROPERTY (symbol-append '% 'create- PROPERTY)))

				      `(let ((property-list (vector-ref properties ,field-idx)))

					 (assert (null? (property-list-read property-list)) (conc ,NAME ": expected an empty list of existing properties for " ',PROPERTY " in unallocated object. We got " (property-list-read property-list)))

					 (for-each
					   (lambda (property)
					     (,%create-PROPERTY node-id seqno property))
					   (property-list-current property-list)))))

				  (object-type-property-specs syntax-type)))))

			(lambda () ; after: This stuff happens whether or not the transaction succeeds.

			  ; Redeserialise all of the attribute values for the
			  ; object and all of its properties in case the
			  ; serialisers modified them. This makes the object look
			  ; like it would if it had been read from the database. We
			  ; should do this even if the transaction fails.

			  (ensure-attributes-reread! metadata-specs  metadata)
			  (ensure-attributes-reread! attribute-specs attributes)

			  ,@(map

			      (lambda (spec)
				(let* ((type            (property-spec-type spec))
				       (PROPERTY        (property-type-name type))
				       (FIELD-NAME      (property-spec-name spec))
				       (TYPE-PROPERTY   (symbol-append TYPE* '- FIELD-NAME)))
				  `(let ((property-list   (,TYPE-PROPERTY object))
					 (attribute-specs (property-type-attribute-specs ,PROPERTY)))

				     (for-each
				       (lambda (property)
					 ; No need to call PROPERTY? because this is enforced at set time.
					 (ensure-attributes-reread!
					   attribute-specs
					   (property-attributes property)))
				       property-list))))

			      (object-type-property-specs syntax-type))))


		      ; Update the state in the object and each property to
		      ; reflect the fact that it everything has been
		      ; successfully allocated. We do this outside the
		      ; transaction to keep the transaction as short as
		      ; possible. This makes the object look like it would if
		      ; it had been read from the databae. We should only do
		      ; this if the transaction succeeds.
		      ;  + Set the allocated flag for each property.
		      ;  + Make sure the property-list for each property looks
		      ;    like it has been reread so that we know which
		      ;    properties to allocate, which to update and which to
		      ;    delete if save-TYPE is ever called on this object.
		      ;  + Set the allocated flag for the object.

		      ,@(map

			  (lambda (spec)
			    (let* ((type            (property-spec-type spec))
				   (field-idx       (property-spec-field-idx spec))
				   (PROPERTY        (property-type-name type)))
			      `(let ((property-list   (vector-ref properties ,field-idx))
				     (attribute-specs (property-type-attribute-specs ,PROPERTY)))

				 (for-each
				   (lambda (property)
				     ; No need to call PROPERTY? because this is enforced at set time.
				     (set! (property-allocated? property) #t))
				   (property-list-current property-list))

				 (set!
				   (property-list-read property-list)
				   (property-list-current property-list)))))

			  (object-type-property-specs syntax-type))


		      (set! (object-allocated? object) #t)


		      ; Finally, stash the object ID that was assigned.
		      (set! (object-id object) (make-object-id node-id seqno))

		      object)))))))))))


; Defines a save-TYPE! procedure.
; Do the query generation for the attribute table at compile time. Call out to
; the property table allocators as needed based on the object schema.
(define-syntax define-object-updater/sqlite
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-object-updater/sqlite NAME TYPE PREDICATE syntax-type)
	 (let* ((metadata-type         (object-type-metadata-type syntax-type))
		(metadata-table-name   (property-type-table metadata-type))
		(metadata-table-query  (UPDATE metadata-table-name
					       `(node-id seqno) ; type column is not needed
					       (property-type-key-specs       metadata-type)
					       (property-type-attribute-specs metadata-type)))
		(attribute-table-name  (object-type-table syntax-type))
		(attribute-table-query (UPDATE attribute-table-name
					       `(node-id seqno)
					       '()
					       (object-type-attribute-specs syntax-type))))
	   `(define ,NAME
	      (let* ((type               ,TYPE)
		     (table              (object-type-table type))
		     (metadata-type      (object-type-metadata-type type))
		     (metadata-table     (property-type-table metadata-type))
		     (metadata-key-specs (property-type-key-specs metadata-type))
		     (metadata-specs     (property-type-attribute-specs metadata-type))
		     (metadata-updater   (make-table-mutator ,metadata-table-query metadata-table metadata-key-specs metadata-specs))
		     (attribute-specs    (object-type-attribute-specs type))
		     (attribute-updater  (make-table-mutator ,attribute-table-query table '() attribute-specs)))
		(lambda (object)

		  (parameterize ((writing-object? #t))

		    (assert (,PREDICATE object)        (conc ,NAME ": expected a " ',TYPE " object. We got " object "!"))
		    (assert (object-allocated? object) (conc ,NAME ": cannot save unallocated object " object))

		    (let* ((metadata   (property-attributes (object-metadata object)))
			   (attributes (object-attributes   object))
			   (properties (object-properties   object))
			   (id         (object-id object))
			   (node-id    (object-id-node-id id))
			   (seqno      (object-id-seqno   id)))

		      (dynamic-wind
			values     ; before
			(lambda () ; thunk

			  (with-gromit-nested-write-transaction
			    (current-gromit-db)
			    (lambda ()

			      ; We end up calling user code such as the attribute-type
			      ; procedures which may lock up whilst trying to allocate
			      ; sequence numbers and things!

			      (metadata-updater  `(,node-id ,seqno) metadata)
			      (attribute-updater `(,node-id ,seqno) attributes)

			      ,@(map

				  (lambda (spec)
				    (let* ((property-name    (property-spec-name spec))
					   (type             (property-spec-type spec))
					   (field-idx        (property-spec-field-idx spec))
					   (PROPERTY         (property-type-name type))
					   (%save-PROPERTY!  (symbol-append '% 'save-   PROPERTY '!))
					   (%create-PROPERTY (symbol-append '% 'create- PROPERTY)))

				      `(let* ((property-list (vector-ref properties ,field-idx))
					      (list-read     (property-list-read property-list))
					      (list-current  (property-list-current property-list)))

					 (if (not (eq? not-read list-current))
					   (let ((n-current     (length list-current)))

					     (assert (not (eq? not-read list-read)) (conc ,NAME ": Refusing to save Property " ',property-name " of type " ',PROPERTY " because the property-list was not read from the database before being updated!"))

					     ; Go though each property in list-read,
					     ; finding and removing it from list-current.
					     (for-each
					       (lambda (property)
						 (let ((new-current (delete property list-current)))

						   (if (eq? list-current new-current)
						     ; property has been removed
						     (assert #f (conc ,NAME ": Property " ',property-name " requires a property value to be deleted but this is not currently implemented!"))
						     ; property may have changed
						     (let ((n-new (length new-current)))

						       (assert (= n-new (sub1 n-current)) (conc ,NAME ": Property " ',property-name " contains " (- n-current n-new) " duplicate entries for " property))

						       (assert (property-allocated? property) (conc ,NAME ": Property " ',property-name " got unallocated property " property " in read list!"))

						       (if (vector-any attribute-value-dirty? (property-attributes property))
							 (begin
							   (printf "Got dirty property ~S\n" property)
							   (,%save-PROPERTY! node-id seqno property))
							 (printf "Got clean property ~S\n" property))

						       (set! list-current new-current)
						       (set! n-current    n-new)))))
					       list-read)

					     ; list-current now contains just the properties that need to be created.
					     (for-each
					       (lambda (property)
						 ; Forbid sharing of properties. Enforcing
						 ; this at save and create time keeps the
						 ; logic simple and contained and means
						 ; that duplicates are tolerated until it
						 ; will cause a problem and that the
						 ; property accessors+modifiers don't have
						 ; to do anything special.
						 (assert (not (property-allocated? property)) (conc ,NAME ": cannot create already allocated property " property "! This might occur because properties have been shared between property-lists or objects."))
						 (,%create-PROPERTY node-id seqno property))
					       list-current))))))

				  (object-type-property-specs syntax-type)))))

			(lambda () ; after: This stuff happens whether or not the transaction succeeds.

			  ; Redeserialise all of the attribute values for the
			  ; object and all of its properties in case the
			  ; serialisers modified them. This makes the object look
			  ; like it would if it had been read from the database. We
			  ; should do this even if the transaction fails.

			  (ensure-attributes-reread! metadata-specs  metadata)
			  (ensure-attributes-reread! attribute-specs attributes)

			  ,@(map

			      (lambda (spec)
				(let* ((type      (property-spec-type spec))
				       (field-idx (property-spec-field-idx spec))
				       (PROPERTY  (property-type-name type)))
				  `(let* ((property-list (vector-ref properties ,field-idx))
					  (attribute-specs (property-type-attribute-specs ,PROPERTY))
					  (list-current    (property-list-current property-list)))

				     (if (not (eq? not-read list-current))
				       (for-each
					 (lambda (property)
					   ; No need to call PROPERTY? because this is enforced at set time.
					   (ensure-attributes-reread!
					     attribute-specs
					     (property-attributes property)))
					 list-current)))))

			      (object-type-property-specs syntax-type))))


		      ; Update the state in each property to reflect the fact
		      ; that it everything has been successfully saved. We do
		      ; this outside the transaction to keep the transaction as
		      ; short as possible. This makes the object look like it
		      ; would if it had been read from the databae. We should
		      ; only do this if the transaction succeeds.
		      ;  + Set the allocated flag for each property. This is
		      ;    harmless for properties that were already allocated
		      ;    and ensures that the flag is set for all newly
		      ;    allocated properties.
		      ;  + Make sure the property-list for each property looks
		      ;    like it has been reread so that we know which
		      ;    properties to allocate, which to update and which to
		      ;    delete if save-TYPE is ever called again on this
		      ;    object.

		      ,@(map

			  (lambda (spec)
			    (let* ((type      (property-spec-type spec))
				   (field-idx (property-spec-field-idx spec))
				   (PROPERTY  (property-type-name type)))
			      `(let* ((property-list   (vector-ref properties ,field-idx))
				      (attribute-specs (property-type-attribute-specs ,PROPERTY))
				      (list-current    (property-list-current property-list)))

				 (if (not (eq? not-read list-current))
				   (begin
				     (for-each
				       (lambda (property)
					 ; No need to call PROPERTY? because this is enforced at set time.
					 (set! (property-allocated? property) #t))
				       list-current)

				     (set!
				       (property-list-read property-list)
				       (property-list-current property-list)))))))

			  (object-type-property-specs syntax-type))


		      object)))))))))))


; Defines a read-TYPE! procedure.
; Do the query generation at compile time. 
(define-syntax define-object-reader/sqlite
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx
	((define-object-reader/sqlite NAME TYPE syntax-type)
	 (let* ((metadata-type        (object-type-metadata-type     syntax-type))
		(TYPE-NAME            (object-type-name              syntax-type))
		(metadata-table-name  (property-type-table           metadata-type))
		(metadata-specs       (property-type-attribute-specs metadata-type))
		(attribute-table-name (object-type-table             syntax-type))
		(attribute-specs      (object-type-attribute-specs   syntax-type))
		(property-specs       (object-type-property-specs    syntax-type))
		(read-query           (SELECT #f metadata-table-name '(node-id seqno #;type) #f ; FIXME: type
					      `(,metadata-table-name  ,metadata-specs)
					      `(,attribute-table-name ,attribute-specs))))
	   `(define ,NAME
	      (let* ((type            ,TYPE)
		     (metadata-type   (object-type-metadata-type     type))
		     (metadata-specs  (property-type-attribute-specs metadata-type))
		     (attribute-specs (object-type-attribute-specs   type))
		     (reader          (make-table-reader ,read-query 2
							 metadata-specs
							 attribute-specs)))
		(lambda (object-id)

		  (let* ((node-id          (object-id-node-id object-id))
			 (seqno            (object-id-seqno   object-id))
			 (all-values       (reader node-id seqno #;(symbol->string ',TYPE-NAME))) ; FIXME: remove the type from the join conditions but keep it in the where clause.
			 (n-all-values     (length all-values)))

		    (assert (<= n-all-values 1) (conc ,NAME ": Expected 0 or 1 result for " node-id ":" seqno ". We got " all-values))
		    (if (= 1 n-all-values)
		      (let* ((all-values       (car all-values))
			     (object-id        (first  all-values))
			     (metadata-values  (second all-values))
			     (attribute-values (third  all-values)))

			(ensure-attributes-reread! metadata-specs  metadata-values)
			(ensure-attributes-reread! attribute-specs attribute-values)

			(make-object
			  object-id        ; id
			  type             ; type
			  (make-property   ; metadata
			    metadata-type   ; type
			    metadata-values ; attributes
			    #t)             ; allocated?
			  attribute-values ; attributes
			  (vector ,@(map (lambda _ `(make-property-list not-read not-read)) property-specs)) ; properties
			  #t))             ; allocated?

		      #f)))))))))))


; Defines a find-TYPE-* procedure.
; Do the query generation at compile time.
; Be careful with this tool! It can generate queries that take a long time to
; run, produce a lot of intermediate results and then result in a few calls to
; the object reader.
(define-syntax define-object-finder/sqlite
  (ir-macro-transformer
    (lambda (stx inject compare)
      (match stx

	((define-object-finder/sqlite SIGNATURE ARGUMENTS TYPE CONDITIONS syntax-type)

	 (assert (list? SIGNATURE) (conc "define-object-finder/sqlite: Expected SIGNATURE to be a list. We got " SIGNATURE))

	 (let* ((NAME             (car SIGNATURE))
		(ARGUMENTS-syntax (cdr SIGNATURE))
		(ARGUMENT-NAMES   (map car ARGUMENTS-syntax))
		(ARGUMENT-NAMES   (inject ARGUMENT-NAMES))
		(TYPE*            (strip-syntax TYPE))
		(read-TYPE        (symbol-append 'read- TYPE*))
		(CONDITIONS       (strip-syntax CONDITIONS))
		(query ; This query can return IDs for objects of any type that matches the criteria.
		  (receive (where table-specs) (match CONDITIONS
						 (('quasiquote CONDITIONS)
						  (WHERE syntax-type ARGUMENT-NAMES CONDITIONS))
						 (('quote      CONDITIONS)
						  (WHERE syntax-type ARGUMENT-NAMES CONDITIONS))
						 (else
						   (assert #f (conc "define-object-finder/sqlite: Expected quoted or quasiquoted expression. We got " CONDITIONS "!"))))

			   (apply
			     SELECT
			     #t                    ; distinct
			     (caar table-specs)    ; Name of the first table. (We assume there is at least one!)
			     '(node-id seqno)
			     where
			     (cdr table-specs))))  ; The table-specs for any remaining tables.

		; TODO: Embed the query in a standard object reading query
		;       using an IN clause. For now we just call out to the
		;       object reader for each result that's returned.
		(query query))

	   `(define ,NAME

	      (let* ((ARGUMENTS            ,ARGUMENTS)
		     (argument-types       (map second ARGUMENTS))
		     (argument-serialisers (map
					     (lambda (type)
					       (compose (attribute-type-serialiser type)
							(attribute-type-setter type)))
					     argument-types))
		     (finder               (make-table-reader ,query 2))) ; 2: node-id and seqno

		(lambda (,@ARGUMENT-NAMES) ; Wrap the real procedure in one with a pretty signature.
		  (apply

		    (lambda argument-values

		      ; We use a transaction here because we require repeatable
		      ; reads. Even tho' any object that exists at the time the query
		      ; is run will still exist at the time the object reader is
		      ; called, there is a slim chance it'll no longer match the
		      ; criteria unless we ask for repeatable reads. OTOH, given that
		      ; the property loader will run later, outside this transaction
		      ; and doesn't do versioned access to the object, by the time
		      ; the user gets around to loading the attributes, the object
		      ; may have changed anyway.
		      (with-gromit-nested-transaction
			(current-gromit-db)
			(lambda ()

			  (let ((results
				  (apply
				    finder
				    (map
				      (lambda (serialiser value)
					(serialiser value))
				      argument-serialisers
				      argument-values))))

			    ; finder returns a list of lists where each list
			    ; contains the object-id followed by different sets
			    ; of attributes. In this case there are no
			    ; attributes.
			    ; For now we call the reader for the appropriate
			    ; type. This means we generate a few more queries
			    ; but simplifies the implementation. finder will
			    ; find objects of any type so the reader may return
			    ; #f if the object-id refers to an object of a
			    ; different type.
			    (reverse
			      (fold
				(lambda (result seed)
				  (match result
				    ((object-id)
				     (let ((object (,read-TYPE object-id)))
				       (if object
					 (cons object seed)
					 seed)))))
				'()
				results))))))

		    ,@ARGUMENT-NAMES
		    '()))))))


	((define-object-finder/sqlite SIGNATURE ARGUMENTS TYPE CONDITIONS)
	 `(define-object-finder/sqlite ,SIGNATURE ,ARGUMENTS ,TYPE ,CONDITIONS ,(##sys#slot (field-type->syntax-type* TYPE) 0)))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
)

