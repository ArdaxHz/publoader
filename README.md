# M+ MD Uploader
## Run the bot from Finland, due to region blocking. Some chapters will show up as unavailable in other regions.

```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

python3 run.py
```



## Contribution
Any pull request to change files other than `manga.json` will be rejected.
If there is a new series to add, add the ids to `manga.json`, following the same format as the rest of the file.

**If a series has multiple languages, extend the md id array with the additional language id.**
#### Check if the id has been added first before opening a pull request.