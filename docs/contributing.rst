============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report `bugs <https://github.com/tetrahedron-technologies/pydhsfw/issues>`_.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Write Documentation
~~~~~~~~~~~~~~~~~~~

The pydhsfw project could always use more documentation, whether as part of the
official pydhsfw docs, in docstrings, or even on the web in blog posts,
articles, and such.

The pretty useful extension `autodoc`_ is activated by default and lets
you include documentation from docstrings. Docstrings can be written in
`Google style`_ (recommended!), `NumPy style`_ and `classical style`_.

This is a concise `example <https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>`_ of how to implement docstrings in the Google style.

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an `issue <https://github.com/tetrahedron-technologies/pydhsfw/issues>`_.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Get Started!
------------

Ready to contribute? Here's how to set up pydhsfw for local development.

1. Fork the `pydhsfw`_ repo on GitHub.

2. Clone your fork locally::

    $ git clone git@github.com:your_name_here/pydhsfw.git

3. Install your local copy into a virtualenv. Assuming you have virtualenvwrapper installed, this is how you set up your fork for local development::

    $ mkvirtualenv pydhsfw
    $ cd pydhsfw/
    $ python setup.py develop

   or if using virtualenv::

    $ virtualenv -p python3.8 .env
    $ source .env/bin/activate
    $ pip install -e .

4. We use pre-commit hooks to ensure code is consistently formatted and passes basic `flake8`_ checks. You can set this up using::

    $ pip install pre-commit
    $ pre-commit install

5. Create a branch for local development::

    $ git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

6. ``[NOT IMPLEMENTED YET]`` When you're done making changes, check that your changes pass the tests::

    $ pytest

   To get pytest, just pip install it into your virtualenv.

7. Commit your changes and push your branch to GitHub::

    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature

8. Submit a pull request through the GitHub website.

Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in HISTORY.rst.
3. The pull request should work for all supported Python versions.

.. _flake8: https://flake8.pycqa.org
.. _pydhsfw: https://github.com/tetrahedron-technologies/pydhsfw
.. _autodoc: http://www.sphinx-doc.org/en/stable/ext/autodoc.html
.. _Google style: https://github.com/google/styleguide/blob/gh-pages/pyguide.md#38-comments-and-docstrings
.. _NumPy style: https://numpydoc.readthedocs.io/en/latest/format.html
.. _classical style: http://www.sphinx-doc.org/en/stable/domains.html#info-field-lists
