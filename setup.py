from setuptools import setup, find_packages
import sys, os

# here = os.path.abspath(os.path.dirname(__file__))
# README = open(os.path.join(here, 'README.rst')).read()
# NEWS = open(os.path.join(here, 'NEWS.txt')).read()

version = '0.1'

install_requires = [
    'SQLAlchemy',
    'numpy',
    'pydap >=3.2.2',
]

setup(name='pydap.handlers.sqlalchemy',
    version=version,
    description="A SQLAlchemy handler for Pydap",
    # long_description=README + '\n\n' + NEWS,
    classifiers=[
      # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    ],
    keywords="sqlalchemy database opendap dods dap data science climate oceanography meteorology'",
    author='Rod Glover',
    author_email='rglover@uvic.ca',
    url='',
    license='MIT',
    packages=find_packages('src'),
    package_dir = {'': 'src'},
    namespace_packages = ['pydap', 'pydap.handlers'],
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    entry_points="""
        [pydap.handler]
        sqlalchemy = pydap.handlers.sqlalchemy:SQLAlchemyHandler
    """,
)
