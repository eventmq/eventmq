"""
derp publisher
"""
import eventmq

if __name__ == "__main__":
    p = eventmq.Publisher()
    p.connect('tcp://127.0.0.1:47330')

    for x in xrange(1, 9999999999+1):
        p.send(bytes(x))

    p.close()
