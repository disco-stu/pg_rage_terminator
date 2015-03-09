# pg_rage_terminator

## About

Background worker able to kill random connections based on a configurable
chance. It's based on Michael Paquier's background worker 'kill_idle'.

Rage backend scan is done using pg_stat_activity.

## Compatible PostgreSQL versions

This worker is compatible with PostgreSQL 9.3 and newer versions.

## Installation

Installation of pg_rage_terminator is done using the following commands:

    USE_PGXS=1 make
    sudo make install

After building the background worker you need change shared_preload_libraries
within postgresql.conf:

    shared_preload_libraries = 'pg_rage_terminator'

## Configuration

Following configuration options (GUC) controls pg_rage_terminator.

*   __pg_rage_terminator.chance__: chance to kill a random backend. Valid values
    are 0 to 100. Where 0 means no backends is killed. 100 ensures every backend
    is killed.

*   __pg_rage_terminator.interval__: defines the interval of "kill" lookups in
    seconds. Valid values are 0 to 3600. Where 0 disables the lookup process
    completely.
