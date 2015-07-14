#coding: utf-8

from app import app
import sys,os

root_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__))))
os.chdir(root_dir)

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
