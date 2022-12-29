from distutils.core import setup

setup(name='sepPython',
      version='0.9',
      description='A library for basic IO )SEP) all in python',
      author='R. Clapp',
      author_email='bob@sep.stanford.edu',
      url="http://zapad.stanford.edu/bob/sep-python",
          packages=['sepPython'],
    install_requires=[ 'google-cloud-storage>=1.34.0',
                       'genericSolver @ git+http://zapad.stanford.edu/bob/python-solver.git@e382f2cf2d531c425c8b65b394613fddad087f2c'
    ]
                       
   )
