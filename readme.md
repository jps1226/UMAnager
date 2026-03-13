\# UMAnager 🐎



A lightweight, local web dashboard for scraping, managing, and sorting weekend horse racing data from Netkeiba.



\### ⚠️ Author's Note \& Disclaimer

Let's get one thing out of the way right now: \*\*I barely know anything about horse racing, odds math, or deep betting strategy.\*\* This project was entirely \*vibe-coded\* as a fun, personal tool to help me keep track of cool horse lineages, monitor my favorite runners, and generate some highly unscientific automated picks so I didn't have to guess blindly. 



Please \*\*do not\*\* take the "Smart Sort" or "Auto-Pick" math too seriously, and absolutely do not use this to make actual financial decisions. It's built for fun, not profit! 



That being said, if you \*do\* know what you're doing when it comes to racing algorithms or web scraping, \*\*I would love your input!\*\* Issues, feedback, and pull requests are highly encouraged.



---



\## ✨ Features



\* \*\*Live Netkeiba Scraping:\*\* Pulls down the current weekend's race cards, including post positions, brackets, weights, and live/predicted odds.

\* \*\*Pedigree Sniper:\*\* Automatically translates Japanese horse names to English and allows you to save specific horses to a "Favorites" or "Watchlist" tracker.

\* \*\*Vibe-Based Auto-Picker:\*\* A dynamic strategy slider that lets you weigh your automated picks between "Chalky/Safe" (favoring low odds) and "Maximum Chaos" (favoring fresh horses with good bloodlines). 

\* \*\*Pop-out OrePro Cheat Sheet:\*\* Generates an "always-on-top" floating window that formats your weekend picks perfectly for rapid ticket-building in OrePro.

\* \*\*Quick Search:\*\* A fast, auto-completing search bar to instantly locate any horse running on the weekend card.



---



\## 🛠️ Tech Stack

\* \*\*Backend:\*\* Python, FastAPI, Uvicorn

\* \*\*Data Processing:\*\* Pandas, BeautifulSoup4, Pykakasi (for Romaji translation)

\* \*\*Frontend:\*\* Vanilla HTML, CSS, JavaScript (No heavy frameworks!)

\* \*\*Scraping:\*\* Requests, \[keibascraper](https://pypi.org/project/keibascraper/)



---



\## 🚀 Installation \& Setup



1\. \*\*Clone the repository:\*\*

&nbsp;  ```bash

&nbsp;  git clone \[https://github.com/YOUR\_USERNAME/umanager.git](https://github.com/YOUR\_USERNAME/umanager.git)

&nbsp;  cd umanager

Install the required Python packages:



Bash

pip install fastapi uvicorn pandas beautifulsoup4 requests pykakasi keibascraper

Run the local server:



Bash

uvicorn server:app --reload

Open the Dashboard:

Open your web browser and go to http://127.0.0.1:8000.



🤝 Contributing

Since I built this mostly on vibes, there is definitely room for improvement!



Did I completely botch the odds-weighting math?



Is there a more efficient way to hit the Netkeiba API without triggering rate limits?



Do you have an idea for a cool new feature?



Feel free to open an issue or submit a pull request!





\*\*\*



\### How to use this:

1\. In your project folder on your computer, create a new text file and name it `README.md`.

2\. Paste the text above into it.

3\. Save it, then run these commands in your terminal to push it up to GitHub:

&nbsp;  ```bash

&nbsp;  git add README.md

&nbsp;  git commit -m "Added project README"

&nbsp;  git push

