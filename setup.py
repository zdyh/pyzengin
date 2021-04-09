from setuptools import setup, find_packages

setup(
    name='pyzengin',
    use_scm_version=True,
    url='https://github.com/zdyh/pyzengin.git',
    author="Deyong Zheng",
    author_email='zhengdy@me.com',
    description='Japanese Bank and Branch Code Data',
    packages=find_packages(),
    package_data={'zengin': ['zengin.db']},
    install_requires=[],
    setup_requires=['setuptools-scm'],
)
