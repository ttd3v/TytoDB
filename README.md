# TytoDB
A primitive columnar database

It is a database designed to work with primitives; the features of columnar databases usually have to be implemented by the developer using the tool. The software may be faster than other options because it trades away convenience for control and performance. 

I only suggest using this project in production if the installed project is a production-ready release. You acknowledge that there will be additional tasks to maintain, and you are comfortable with the details you may need to handle.

I am mentioning "details" in a way that remembers how low-level it is treated, but essentially, you just have to be very schema-aware, fathom, and plan the types you're picking since the rows are always fixed-sized.

The system may offer benefits, but sincerely, I think that you shouldn't use it without a clear intent. Using battle-tested options like Postgres, SQLite, and many other options might be more advisable than this new implementation. The one that maybe just me — The guy who wrote, and a stinky nerd (I'm not one, I promise), might fully acknowledge.

# Compatibility
It relies heavily on IO Uring; that said, only Linux systems with kernel version 5.1+ can run the program. Although not mandatory, I suggest running it on an x86_64 machine since I wrote the software with this ISA in mind. Which means it might be slightly more efficient in it.

# Security
The security is user-dependent; it uses FalcoTCP as the networking layer, which means you also rely on its quirks (pre-shared keys, you must manage them well, or you're exposed). Besides networking, the database does not implement disk encryption; you must handle it yourself. Some software does it for you.

For vulnerabilities, reach me at this email: tytodb@proton.me — Please, state in the email subject/title/header that the mail content covers such a topic.

# Documentation
It exists, but I ain't linking it here because I am highly unsatisfied with the past versions as it doesn't match the current state of the project. If you want to take a glance at it, look at the git history.

The old website is unlinked to this page, but when the next release happens, a new and polished documentation will be published. And linked here, of course.

# Interactions
## Contributions
If you are interested in contributing to the project or helping it, raise an issue pointing out a problem, a coherent discussion, a good pull request, or email me at this address: (tytodb@proton.me).
## Suggestions
Send me at this email (tytodb@proton.me), create a discussion, or coherently reach me wherever you prefer.

### Personal notes
This project is meant to be a high-performance database, with good reliability, and excelling at throughput. Yet, claiming those things are complicated, if my code breaks due to a dumb error or what is considered through the claim bounds isn't what I mean, then the claim is shattered. Due to that, expect that the database will be fast, maybe faster than other engines, but I cannot guarantee that. Especially while the project is in development and in early versions. Do not use the project thinking it works, I have no tests yet that can guarantee it is as reliable as SQLite, Postgres, et cetra. They are battle-tested.
If you use the software in this early stage, thank you, and if possible, stretch it, and try to scatter it. That is how robustness grows, I guess.

Thanks for reading.
