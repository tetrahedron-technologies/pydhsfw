===============
Getting Started
===============

By itself **pydhsfw** isn't much use. It's really intended to abstract away various gory details of communicating with the central beamline control system, `DCSS`_ (Distributed Control System Server) written and developed at Stanford University in the `Macromolecular Crystallography Group`_ at `SLAC`_. This enables beamline staff to easily and efficiently write new DHSs (Distributed Hardware Servers).

Perhaps the best way to explain how **pydhsfw** works is to show how it was used to write a working DHS. For example `loopDHS`_ is a DHS that was written to use Google's `AutoML`_ Machine Learning to detect sample loops in a jpeg image and return bounding box information back to the DCSS control system. This is an essential step in the implementation of fully automatic Macromolecular Crystallographic (MX) data collection.

In theory **pydhsfw** could be extended to perform any DCSS operation, control motors & shutters, or read ion chambers data. The main benefit is that it is written in python and might be more accessable to developers not familiar with the native `Tcl/Tk`_ and `C++`_ used by SSRL.

.. _Macromolecular Crystallography Group: https://www-ssrl.slac.stanford.edu/smb-mc/
.. _SLAC: https://www-ssrl.slac.stanford.edu
.. _DCSS: https://www-ssrl.slac.stanford.edu/smb-mc/node/1641
.. _loopDHS: https://loop-dhs.readthedocs.io
.. _AutoML: https://cloud.google.com/vision/automl/docs
.. _Tcl/Tk: https://www.tcl.tk
.. _C++: https://www.cplusplus.com
