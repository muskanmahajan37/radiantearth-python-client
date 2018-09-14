radiantearth-python-client
============================

A Python client for `Radiant Earth <https://doc.radiant.earth/>`_, a web platform for combining, analyzing, and publishing raster data.

Usage
-----

.. code-block:: python

   from radiantearth.api import API
   refresh_token = '<>'

   api = API(refresh_token=refresh_token)

   # List all projects
   my_projects = api.projects

   one_project = my_projects[0]

   # Get TMS URl without token
   one_project.tms()

Versions
~~~~~~~~

The latest version of ``radaintearth`` always points to the most recently released swagger spec in
the raster-foundry/raster-foundy-api-spec repository. If you need to point to a different spec
version, either install a version of the python client that refers to the appropriate spec, or
set the ``RE_API_SPEC_PATH`` environment variable to a url or local file path pointing to the
version of the spec that you want to use.

Generally this shouldn't matter, because the Radiant Earth API shouldn't have breaking changes.


Installation
------------

Without notebook support
~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   $ pip install radiantearth

With notebook support
~~~~~~~~~~~~~~~~~~~~~

Notebook support requires `npm <https://www.npmjs.com/get-npm>`_.

.. code:: bash

   $ pip install radiantearth[notebook]

Then, enable widgets and leaflet in in jupyter notebooks:

.. code:: bash

   $ jupyter nbextension install --py --symlink --sys-prefix widgetsnbextension
   $ jupyter nbextension enable --py --sys-prefix widgetsnbextension 
   $ jupyter nbextension install --py --symlink --sys-prefix ipyleaflet
   $ jupyter nbextension enable --py --sys-prefix ipyleaflet


Testing
-------

The test suite execution process is managed by ``tox``:

.. code:: bash

   $ tox


Contribute
----------

If you find this client useful and would like to contribute, make a pull request or send us an email at ``code@radiant.earth``.
