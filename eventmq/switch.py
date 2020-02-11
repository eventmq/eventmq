import eventmq

if __name__ == "__main__":
    switch = eventmq.Switch()
    # switch.listen('tcp://127.0.0.1:47331', 'tcp://127.0.0.1:47330')
    try:
        switch.start()
    except Exception:
        switch.stop()
