# filefinder

> Find and parse file and folder names.

Define regular folder and file patterns with the intuitive python syntax:

```python
from filefinder import FileFinder

path_pattern = "/root/{category}"
file_pattern = "{category}_file_{number}"

ff = FileFinder(path_pattern, file_pattern)
```

Everything enclosed in curly brackets is a placeholder. Thus, you can create file and
path names like so:

```python
ff.create_path_name(category="a")
>>> /root/a/

ff.create_file_name(category="a", number=1)
>>> a_file_1

ff.create_full_name(category="a", number=1)
>>> /root/a/a_file_1
```

However, the strength of filefinder is parsing file names on disk. Assuming you have the
following folder structure:

```
/root/a1/a1_file_1
/root/a1/a1_file_2
/root/a2/a2_file_1
/root/a2/a2_file_2
/root/a3/a2_file_1
/root/a3/a2_file_2
```

You can then look for paths:

```python
ff.find_paths()
>>> <FileContainer>
>>>      filename category
>>> 0  /root/a1/*       a1
>>> 1  /root/a2/*       a2
>>> 2  /root/a3/*       a3
```
The placeholders (here `{category}`) is parsed and returned. You can also look for
files:

```python
ff.find_files()
>>> <FileContainer>
>>>              filename category number
>>> 0  /root/a1/a1_file_1       a1      1
>>> 1  /root/a1/a1_file_2       a1      2
>>> 2  /root/a2/a2_file_1       a2      1
>>> 3  /root/a2/a2_file_2       a2      2
>>> 4  /root/a3/a2_file_1       a3      1
>>> 5  /root/a3/a2_file_2       a3      2
```

It's also possible to filter for certain files:
```python
ff.find_files(category=["a1", "a2"], number=1)
>>> <FileContainer>
>>>              filename category number
>>> 0  /root/a1/a1_file_1       a1      1
>>> 2  /root/a2/a2_file_1       a2      1
```
