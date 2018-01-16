from setuptools import setup, find_packages

setup(
    name="kafka",
    version="0.1",
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    # setup_requires=["numpy"],
    # install_requires=['numpy'],
    entry_points={
        'console_scripts': [
            'foo = replica.cmd:main',
            # 'bar = demo:test',
        ],
        # 'gui_scripts': [
        #     'baz = demo:test',
        # ]
    }
)
