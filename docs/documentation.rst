Documentation
=============

Building
########

Docs are generated with `Sphinx <http://www.sphinx-doc.org/en/stable/>`_
with the help of `autodoc <http://www.sphinx-doc.org/en/stable/ext/autodoc.html>`_.
To generate the documentation::

    cd docs
    make html

Publishing
##########

Docs are hosted on github pages from the `gh-pages` branch of the repository.
To publish the lastest docs, first make them with sphinx, add them to the
`gh-pages` branch with `ghp-import`, then push `gh-pages` to the remote::

    cd dcos
    make html
    ghp-import build/html
    git push origin gh-pages


Viewing Changes Locally
#######################

Sphinx-autobuild can be run in the background to rebuild the site and run
a server while you make changes to the docs::

    cd docs
    sphinx-autobuild ./ build
