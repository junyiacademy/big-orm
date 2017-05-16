from distutils.core import setup
# use >> python setup.py sdist upload     to upload new version

setup(
    name='big-orm',    # This is the name of your PyPI-package.
    packages=['big_orm'],
    version='0.031',                          # Update the version number for new releases
    description='ORM for bigquery, reuse and maintain bigquery sql more easily',
    author='ENsu',
    author_email='ensu@junyiacademy.org',
    install_requires=[
        "google-api-python-client",
        "pandas",
        "retrying",
        "ibis-framework"
    ]
)
