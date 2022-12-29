from distutils.core import setup

setup(name='sep-python',
      version='0.9',
      description='A library for basic IO )SEP) all in python',
      author='R. Clapp',
      author_email='bob@sep.stanford.edu',
      url="http://zapad.stanford.edu/bob/sep-python",
          packages=['sep-python'],
    install_requires=[ 'google-cloud-storage>=2.4.0',
                       'genericSolver @ git+https://zapad.stanford.edu/bob/python-solver'
    ]
                       
   )
