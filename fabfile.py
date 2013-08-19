from fabric.api import *

env.use_ssh_config = True

env.hosts = ['login.genome.au.dk']


@task
def setup():
    local('python setup.py sdist')

    put('dist/*.tar.gz')
    put('example/')

    run('rm -rf ~/.example/ ~/.local/')

    run('tar xf gwf-0.1.0.tar.gz')
    with cd('gwf-0.1.0'):
        run('python setup.py install --force --user')


@task
def test_workflow_gwf():
    run('~/.local/bin/gwf -n RealWorkflow -w 0:10:0 -f example/workflow.gwf SortBAM')


@task
def test_very_simple_gwf():
    run('~/.local/bin/gwf -n VerySimple -w 0:10:0 -f example/very_simple.gwf TargetB')
