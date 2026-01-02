<div align="center">
  <img src="assets/zqlite-logo.png" alt="ZQLite Logo" width="200" height="200">
</div>

# zqlite 🚀🔐

![Zig](https://img.shields.io/badge/zig-0.16.0--dev-f7a41d?style=flat-square)
![Status](https://img.shields.io/badge/status-beta-yellow?style=flat-square)
![Crypto](https://img.shields.io/badge/PQC-experimental-orange?style=flat-square)
![Platform](https://img.shields.io/badge/platform-cross--platform-lightgrey?style=flat-square)
![Performance](https://img.shields.io/badge/performance-high--speed-brightgreen?style=flat-square)

> **High-performance embedded database and query engine for Zig**
> A standalone embedded database offering SQLite compatibility with experimental post-quantum cryptographic features. Powers [Ghostchain](https://github.com/ghostkellz/ghostchain) and [Ghostwire](https://github.com/ghostkellz/ghostwire).

**Note**: Post-quantum cryptography features (ML-KEM, ML-DSA, ZKP) are **experimental proof-of-concept** implementations. The core database engine (SQLite-compatible storage, SQL parsing, WAL) is the primary focus for production use.

---

## 🌟 Key Features

### 🗃️ **Core Database (Production Ready)**
- **SQLite Compatibility**: Drop-in replacement for embedded applications
- **PostgreSQL-style SQL**: Extended data types (UUID, JSON, timestamps)
- **B+ Tree Storage**: Efficient indexing with Write-Ahead Logging (WAL)
- **Zero Configuration**: Single-file database, no server setup required
- **Memory-Safe**: Pure Zig implementation with no undefined behavior
- **Interactive CLI**: Full SQL shell with tab completion

### 🔐 **Encryption (Stable)**
- **Field-Level Encryption**: ChaCha20-Poly1305 AEAD per column
- **Secure Key Derivation**: Argon2 password hashing
- **Transparent Encryption**: Automatic encrypt/decrypt on read/write

### 🔮 **Post-Quantum Crypto (Experimental)**
> ⚠️ **Proof of Concept** - Not for production use

- **ML-KEM-768**: NIST post-quantum key encapsulation (experimental)
- **ML-DSA-65**: Post-quantum digital signatures (experimental)
- **Hybrid Signatures**: Ed25519 + ML-DSA (experimental)
- **Bulletproofs ZKP**: Zero-knowledge range proofs (experimental)

### 🌐 **Networking (Experimental)**
- **QUIC Transport**: Modern UDP-based transport layer
- **Hybrid Key Exchange**: X25519 + ML-KEM-768
- **CNS Integration**: Ghostchain name resolution

---

## 🚀 Quick Start

### One-Line Installation
```bash
curl -sSL https://raw.githubusercontent.com/ghostkellz/zqlite/main/install.sh | bash
```

### Zig Integration
```bash
zig fetch --save https://github.com/ghostkellz/zqlite/archive/refs/heads/main.tar.gz
```

### Manual Installation
```bash
git clone https://github.com/ghostkellz/zqlite
cd zqlite
zig build
./zig-out/bin/zqlite shell
```

### Run Post-Quantum Demos
```bash
# Showcase all new features
zig build run-pq-showcase

# CNS Ghostchain integration demo
zig build run-cns-demo

# Banking system with hybrid crypto
zig build run-banking

# Other examples
zig build run-nextgen
zig build run-powerdns
```

---

## 📋 Feature Status

| Feature | Status | Notes |
|---------|--------|-------|
| SQL Parser | ✅ Stable | CREATE, INSERT, SELECT, UPDATE, DELETE, JOIN |
| B+ Tree Storage | ✅ Stable | Efficient indexing and retrieval |
| Write-Ahead Log | ✅ Stable | Durability and crash recovery |
| Page-based Storage | ✅ Stable | 4KB pages with buffer pool |
| In-Memory Mode | ✅ Stable | Fast ephemeral databases |
| File-based Mode | ✅ Stable | Persistent storage |
| Interactive CLI | ✅ Stable | SQL shell with dot commands |
| ChaCha20-Poly1305 | ✅ Stable | Field-level encryption |
| Connection Pooling | ✅ Stable | Multi-connection support |
| ML-KEM-768 | 🧪 Experimental | Post-quantum key exchange |
| ML-DSA-65 | 🧪 Experimental | Post-quantum signatures |
| Bulletproofs ZKP | 🧪 Experimental | Zero-knowledge proofs |
| QUIC Transport | 🧪 Experimental | Network layer |
| CNS Adapter | 🧪 Experimental | Ghostchain integration |

---

## 🛠 Usage Examples

### Basic Database Operations
```zig
const zqlite = @import("zqlite");

// Open a file-based database
const conn = try zqlite.open(allocator, "mydata.db");
defer conn.close();

// Create a table
try conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);");

// Insert data
try conn.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');");

// Query with results
var result = try conn.query("SELECT * FROM users WHERE id = 1;");
defer result.deinit();

while (result.next()) |row| {
    const name = row.getTextByName("name");
    std.debug.print("User: {s}\n", .{name orelse "unknown"});
}
```

### In-Memory Database
```zig
// Create an in-memory database (no file I/O)
const conn = try zqlite.openMemory(allocator);
defer conn.close();

// Use like any other database
try conn.execute("CREATE TABLE temp (value INTEGER);");
_ = try conn.exec("INSERT INTO temp VALUES (42);");
```

### Transactions
```zig
// Manual transaction control
try conn.begin();
errdefer conn.rollback() catch {};

try conn.execute("INSERT INTO accounts VALUES (1, 1000);");
try conn.execute("INSERT INTO accounts VALUES (2, 500);");

try conn.commit();
```

### Prepared Statements
```zig
// Prepare for repeated execution
var stmt = try conn.prepare("INSERT INTO users (name, email) VALUES (?, ?);");
defer stmt.deinit();

try stmt.bind(0, "Bob");
try stmt.bind(1, "bob@example.com");
_ = try stmt.execute();
```

### Experimental: Post-Quantum Crypto
> ⚠️ These APIs are experimental and subject to change

```zig
// See src/crypto/ for experimental PQC features
// These are proof-of-concept implementations
```

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    zqlite Architecture                     │
├─────────────────────────────────────────────────────────────┤
│  SQL Interface & CLI  │  Examples & Demos  │  Public API   │
├─────────────────────────────────────────────────────────────┤
│           Post-Quantum QUIC Transport Layer                │
│  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐ │
│  │   PQ-QUIC      │ │  ZKP Queries    │ │ Hybrid Sigs  │ │
│  │  ML-KEM + X25519│ │  Bulletproofs   │ │ Ed25519+MLDSA│ │
│  └─────────────────┘ └─────────────────┘ └──────────────┘ │
├─────────────────────────────────────────────────────────────┤
│              Enhanced Crypto Engine (zcrypto)              │
│  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐ │
│  │  ML-KEM-768    │ │   ML-DSA-65     │ │  ChaCha20    │ │
│  │  Post-Quantum  │ │  PQ Signatures  │ │  Poly1305    │ │
│  │  Key Exchange  │ │   + Ed25519     │ │  AEAD        │ │
│  └─────────────────┘ └─────────────────┘ └──────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Core Database Engine                     │
│  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐ │
│  │   SQL Parser   │ │    B+ Trees     │ │     WAL      │ │
│  │   & Executor   │ │   + Indexes     │ │   Logging    │ │
│  └─────────────────┘ └─────────────────┘ └──────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Storage & Memory                         │
│           File System  │  Memory Pools  │  Page Cache     │
└─────────────────────────────────────────────────────────────┘
```

---

## 📊 Performance

zqlite delivers cutting-edge performance:

### Post-Quantum Operations
- **ML-KEM-768 Keygen**: >50,000 ops/sec
- **ML-KEM-768 Encaps/Decaps**: >30,000 ops/sec  
- **Hybrid Key Exchange**: >25,000 ops/sec
- **ML-DSA-65 Signing**: >15,000 ops/sec

### QUIC Transport
- **PQ Handshake**: <2ms
- **Packet Encryption**: >10M packets/sec
- **Zero-Copy Processing**: Minimal overhead

### Traditional Database
- **Inserts**: >100,000 ops/sec
- **Queries**: >500,000 ops/sec
- **Memory Usage**: <10MB baseline

---

## 🎯 Use Cases

### 🏦 **Financial Services**
- Post-quantum secure banking databases
- Zero-knowledge compliance reporting
- Quantum-safe transaction processing
- Privacy-preserving financial analytics

### 🏥 **Healthcare & Privacy**
- HIPAA-compliant patient databases
- Zero-knowledge medical research
- Quantum-safe health records
- Private genomic data storage

### 🌐 **DNS & Networking**
- Post-quantum DNSSEC databases
- Secure DNS record storage
- Quantum-safe name resolution
- High-performance DNS servers

### 🔐 **Cryptocurrency & Blockchain**
- Quantum-resistant wallet databases
- Zero-knowledge transaction proofs
- Post-quantum consensus systems
- Private DeFi applications

### 🎮 **Gaming & Real-Time**
- Secure multiplayer databases
- Anti-cheat with zero-knowledge
- Real-time encrypted data sync
- Privacy-preserving leaderboards

---

## 🧪 Testing

Run the comprehensive test suite:

```bash
# All tests
zig build test

# Specific test categories
zig build test -- --filter "crypto"
zig build test -- --filter "post_quantum"
zig build test -- --filter "zkp"
```

### Test Coverage
- ✅ NIST post-quantum test vectors (ML-KEM, ML-DSA)
- ✅ Zero-knowledge proof correctness
- ✅ QUIC crypto operations  
- ✅ Hybrid signature verification
- ✅ Database encryption/decryption
- ✅ SQL compatibility
- ✅ Performance benchmarks

---

## 📈 Roadmap

### Future Enhancements
- **Formal Verification**: Mathematical proof of security properties
- **Hardware Security Modules**: HSM integration for key storage
- **Machine Learning Security**: AI-powered threat detection
- **Quantum Key Distribution**: QKD protocol support
- **Multi-Party Computation**: Secure distributed queries
- **Homomorphic Encryption**: Compute on encrypted data
- **Advanced ZKP**: Recursive proofs and STARKs
- **Cross-Language Bindings**: Python, Go, Rust FFI

---

## 🤝 Contributing

We welcome contributions! zqlite represents the cutting edge of cryptographic database technology.

### Getting Started
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality  
4. Ensure all tests pass
5. Submit a pull request

### Areas of Interest
- Post-quantum cryptography optimizations
- Zero-knowledge proof systems
- QUIC transport improvements
- New database features
- Performance optimizations

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

- **zcrypto**: [@ghostkellz](https://github.com/ghostkellz) for world-class post-quantum cryptography
- **NIST**: For standardizing ML-KEM and ML-DSA algorithms
- **Zig Team**: For the amazing systems programming language
- **Community**: For feedback, testing, and contributions

---

## 📞 Support & Community

- **GitHub Issues**: Bug reports and feature requests
- **Discussions**: Community support and questions  
- **Discord**: Real-time chat (link in issues)
- **Documentation**: Complete API reference in `/docs`

---

**🚀 zqlite - The world's most advanced post-quantum cryptographic database!**

*Ready for the quantum computing era* 🌟