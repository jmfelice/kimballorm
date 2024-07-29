==========
KimballORM
==========






This project provides the ability to manage a data warehous using sqlalchmey and alembic.  Importantly, this data warehouse is a dimensional model that follows the Ralph Kimball data warehousing philosphy.  As a result each table in the ORM is defined with the appropriate type of dimension or fact table it represents.  Further each type of table inherits certain functions that better allow for the automation of CRUD scripts to be deployed to the database.



Features
--------

* TODO

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
