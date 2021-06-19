
from setuptools import setup #, find_packages

with open("requirements.txt") as f:
    install_requires = f.read().splitlines()


setup(
    name='IWIND-LR-TOOLS',
    version='0.0.3',
    url='https://github.com/yiyuezhuo/IWIND-LR-TOOLS.git',
    author='yiyuezhuo',
    author_email='yiyuezhuo@gmail.com',
    description='Tools to create, manipuate and analyze IWIND-LR input and output file.',
    packages=["iwind_lr_tools"],    
    install_requires=install_requires,
)
