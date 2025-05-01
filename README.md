# 🗝️ First Go Project: Key-Value Server & Squarer

This project implements two Go components: a centralized key-value server (`p0partA`) and a concurrent squaring mechanism (`p0partB`). It demonstrates the use of Go's concurrency features, such as goroutines and channels, to build a networked server. Also in the p0partB, I worked with the testing mechanisms in Go.

---

## 📌 Project Overview

- **Part A: Key-Value Server (`p0partA`)**:
  - A TCP-based server that supports multiple clients for key-value operations (Put, Get, Delete, Update).
  - Handles slow clients by buffering messages and dropping them when necessary.
  - Tracks active and dropped client connections.
- **Part B: Squarer (`p0partB`)**:
  - A concurrent squaring mechanism that takes an input channel of integers and outputs their squares on another channel.

---

## 🧰 Tools Used

- [Go](https://golang.org/) for implementation and testing
- [Git](https://git-scm.com/) for version control
- Standard Go testing framework (`go test`)

---

## 🚀 How to Run

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/kasra-noorbakhsh/First-Go-Project.git
   cd First-Go-Project
   ```
   
   ```bash
   Run Tests for Part A (Key-Value Server):
   cd p0partA
   go test -v
   ```

   ```bash
   Run Tests for Part B (Squarer):
   cd ../p0partB
   go test -v
   ```

---

## 📬 Contact

Made by **Kasra Noorbakhsh**  
📧 Feel free to connect or provide feedback!
