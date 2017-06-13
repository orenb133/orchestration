from setuptools import setup

setup(name='orchestration',
      version='0.1',
      description = "A simple event driven multiprocess framework",
      packages=['orchestration'],
      package_data={'orchestration': ['default.conf']},
      include_package_data=True,
      install_requires=['msgpack-python', 
                        'aenum', 
                        'pyzmq', 
                        'influxdb',
						'yappi'])