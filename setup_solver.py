from distutils.core import setup

setup(name='genericSolver',
      version='1.0',
      description='SEP Python solver',
      author='E.Biondi, ...., Clapp',
      author_email='bob@sep.stanford.edu',
      url='http://zapad.stanford.edu/ettore88/python-solver',
          packages=['genericSolver'],
    install_requires=[ 'numpy>=1.20.1', 'pylops>=2.0.0', 
                       'h5py>=2.10.0', 'distributed>=2022.1.1','dask-jobqueue>=0.7.5',
                      'matplotlib>=3.3.4', 'scipy>=1.4.2']
   )
