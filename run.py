

from app import app
import sys

if __name__ == '__main__':
    # print sys.path

    app.run(debug = True,host="0.0.0.0",port=5000)
