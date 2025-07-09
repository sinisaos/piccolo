===========
Constraints
===========

Simple unique constraints
=========================

Unique constraints can be added to a single column using the ``unique=True``
argument of ``Column``:

.. code-block:: python

    class Band(Table):
        name = Varchar(unique=True)

-------------------------------------------------------------------------------

Multi-column (composite) unique constraints
===========================================

To manually create and drop multi-column unique constraints, we can use Piccolo's 
``raw`` method in migrations or script.

If you are using automatic migrations, we can specify the ``UniqueConstraint``
argument and they handle the creation and deletion of these unique constraints. 

.. currentmodule:: piccolo.constraint

.. autoclass:: UniqueConstraint