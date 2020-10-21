from setuptools import setup

setup(name='flightstats_api_client',
      version='0.1',
      description='Provides a Python client to FlightStats.com\'s flight status API',
      author='Daniel W. Chen',
      author_email='s1300045@gmail.com',
      license='MIT',
      packages=['flightstats_api_client',],
      install_requires=[
        'requests',
      ],
      zip_safe=False)
