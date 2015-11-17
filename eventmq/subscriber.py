"""
derp subscriber
"""
import eventmq

if __name__ == "__main__":
    s = eventmq.Subscriber()
    s.connect('tcp://127.0.0.1:47331')
    s.subscribe('/topic1')

    while True:
        # block until something comes in. normally you'd do something with
        # this in another thread or something
        print 'r'
        print s.receive()
