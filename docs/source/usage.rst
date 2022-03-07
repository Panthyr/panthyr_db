===============================
Panthyr DB example code
===============================

Example code:

# TODO

.. code:: python

    >>> from xxx import YYY

    >>> p = PTHeadIPConnection(ip = '192.168.100.190')

    >>> h.initialize()
    True

    >>> h.send_cmd('TP-1350')  # move tilt to position -1350
    >>> h.current_pos()  # query head position
    ['4002', '-1350']
