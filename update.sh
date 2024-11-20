#! /bin/bash
pip install uv
uv pip compile setup.cfg -o requirements.txt --universal
uv pip compile dev-requirements.in -o dev-requirements.txt --universal