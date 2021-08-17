#########
GIT
#########
rm -r .git
git init
git config user.name "dietervansteenwegen"
git config user.email github@vansteenwegen.org
git remote add origin git_prive:Panthyr/xxxx
git push -u origin main

#########
SPHINX
#########

`cd docs`
`sphinx-quickstart`

* copy conf.py parts
* add modules and usage to index.rst

########
VENV
########

`python3 -m virtualenv --python python3 venv`

`.\\venv\\Scripts\\activate`


########
Others
########
* Write **README.md**
