# TytoDB
A primitive columnar database

It is a database mean't to work uppon primitives, the features columnar databases usually have to be implemented by the developer using the tool. The software may be faster than other options because it trades away convenhience for control, and performance. 

I only suggest you using this project in production, if the installed project is a production-ready-release, you acknowledge that there will be more to maintain, and are okay with the details you might have to handle.

I am mentining "details" in a way that remember how low-level is treated, but essentially, you just have to be very schema aware, fathom, and plan the types you're picking since the rows are always fixed sized.

The system may offer benefits, but sincerelly, I think that you shouldn't use it without clear intent, using battle-tested options like Postgres, SQlite, and many other options might be more advisable than this new implementation. The one that maybe just me — The guy who wrote, and a stinky nerd (I'm not one, I promisse), might fully acknowledge.

# Compatibility
It relies heavily on IO Uring, that said, only linux systems with kernel version 5.1+ can run the program. Although not mandatory, I suggest running it on a x86_64 machine since I wrote the software with this ISA in mind. Which means it might be slightly more efficient in it.

# Security
The security is user-dependent, it uses FalcoTCP as the networking layer, which means you also rely on it's quirks (pre-shared keys, must manage them well, or you're exposed). Besides networking, the database does not implement disk-encryption, you must handle it yourself. There are some softwares that does it for you.

For vulnerabilities reach me at this email: tytodb@proton.me — Please, state at the email subject/title/header that the mail content cover such a topic.

# Documentation
It exists, but I ain't linking it here because I am highly unsatisfied with the past versions as it doesn't match the current state of the project. If you want to take a glance at it, look at git history.

The old website is unlinked to this page, but when the next release happens, a new and polished documentation will be published. And linked here, of course.

# Interactions
## Contributions
If you are interested in contributing to the project, or helping it, raise an issue pointing a problem, coherent discussion, a good pull-request, or email me at this address: (tytodb@proton.me).
## Suggestions
Send me at this email (tytodb@proton.me), create a discussion, or coherently reach me wherever you prefer.

### Personal notes
This project is meant to be a high-performance database, with good reliability, and exceling at throughput. Yet, claiming those things are compliated, if my code breaks due to a dumb error or what is considered through the claim bounds isn't what I mean, then the claim is shattered. Due to that, expect that the database will be fast, maybe faster than other engines, but I cannot guarantee that. Specially while the project is in development and in early versions. Do not use the project thinking it works, I have no tests yet that can guarantee it is as reliable as SQLite, Postgres, et cetra. They are battle-tested.
If you use the software in this early stage, thank you, and if possible, strech it, and try to scatter it. That is how robustness grows, I guess.

Thanks for reading.
