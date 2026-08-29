import sys
import os
import typing

if "typing.io" not in sys.modules:
    sys.modules["typing.io"] = typing
if "typing.re" not in sys.modules:
    sys.modules["typing.re"] = typing

# Configure JAVA_HOME to JDK 17 if local installation exists
custom_jdk17 = os.path.expanduser("~/.jdk17")
if os.path.isdir(custom_jdk17) and "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = custom_jdk17
    os.environ["PATH"] = f"{custom_jdk17}/bin:{os.environ.get('PATH', '')}"