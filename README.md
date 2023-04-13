# Official Publisher MangaDex Uploader
## Works by reading new updates provided by the extensions. Extensions can be of any free-to-read chapters publisher.
### Tested on Python 3.9+

---

#### To run the scheduler:

```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

python3 run.py
```

#### To run the bot by itself:
```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

python3 -m publoader.publoader
```

## Contributing
If there is something you think needs changing, open an issue or a PR with your changes. Format the code using the [Black](https://pypi.org/project/black/) formatter with the default args.


## Extensions
If there is a publisher missing, you can make your own extension. Check the [extensions readme](publoader/extensions/CONTRIBUTING.md) for how to do so.