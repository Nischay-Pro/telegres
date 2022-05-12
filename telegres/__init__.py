#!/usr/bin/env python
#
# A library that extends the functionality of Python Telegram Bot
# by providing more Persistence classes.
# Copyright (C) 2022
# Nischay Ram Mamidi <NischayPro@protonmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser Public License for more details.
#
# You should have received a copy of the GNU Lesser Public License
# along with this program.  If not, see [http://www.gnu.org/licenses/].

from ._postgrespersistence import PostgresPersistence
from . import _version

__version__ = _version.get_versions()["version"]
