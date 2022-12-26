
solverPip:
	rm -rf /tmp/buildS
	mkdir /tmp/buildS
	cp python-solver/LICENSE python-solver/README.md  /tmp/buildS
	cp -r python-solver/GenericSolver/python /tmp/buildS/genericSolver
	cp setup_solver.py /tmp/buildS/setup.py
	cp solver_init.py /tmp/buildS/genericSolver/__init__.py
	cd /tmp/buildS && \
	python setup.py check &&\
	python setup.py sdist

