const std = @import("std");

/// ZQLite v0.6.0 Crypto Abstraction Layer
/// Supports multiple backends: native (std.crypto), shroud, none
pub const CryptoBackend = enum {
    native, // Zig std.crypto (default, no dependencies)
    shroud, // Shroud library (when available)
    none, // Disabled crypto features
};

pub const CryptoConfig = struct {
    backend: CryptoBackend = .native,
    enable_pq: bool = false, // Post-quantum crypto (requires shroud)
    enable_zkp: bool = false, // Zero-knowledge proofs (requires shroud)
    hybrid_mode: bool = true, // Classical + PQ hybrid
};

/// Unified crypto interface - backend agnostic
pub const CryptoInterface = struct {
    backend: CryptoBackend,
    config: CryptoConfig,

    const Self = @This();

    pub fn init(config: CryptoConfig) Self {
        return Self{
            .backend = config.backend,
            .config = config,
        };
    }

    /// Generate secure random bytes
    pub fn randomBytes(self: Self, buffer: []u8) !void {
        switch (self.backend) {
            .native => {
                std.crypto.random.bytes(buffer);
            },
            .shroud => {
                // Fallback to native
                std.crypto.random.bytes(buffer);
            },
            .none => {
                @memset(buffer, 0); // Insecure fallback
            },
        }
    }

    /// Hash function (SHA-256)
    pub fn hash(self: Self, data: []const u8, output: *[32]u8) !void {
        switch (self.backend) {
            .native => {
                var hasher = std.crypto.hash.sha2.Sha256.init(.{});
                hasher.update(data);
                hasher.final(output);
            },
            .shroud => {
                // Fallback to native
                var hasher = std.crypto.hash.sha2.Sha256.init(.{});
                hasher.update(data);
                hasher.final(output);
            },
            .none => {
                @memset(output, 0); // Insecure fallback
            },
        }
    }

    /// HKDF key derivation
    pub fn hkdf(self: Self, ikm: []const u8, salt: []const u8, info: []const u8, output: []u8) !void {
        switch (self.backend) {
            .native => {
                const Hkdf = std.crypto.kdf.hkdf.HkdfSha256;
                const prk = Hkdf.extract(salt, ikm);
                Hkdf.expand(output, info, prk);
            },
            .shroud => {
                // Fallback to native
                const Hkdf = std.crypto.kdf.hkdf.HkdfSha256;
                const prk = Hkdf.extract(salt, ikm);
                Hkdf.expand(output, info, prk);
            },
            .none => {
                @memset(output, 0); // Insecure fallback
            },
        }
    }

    /// Symmetric encryption (ChaCha20-Poly1305)
    pub fn encrypt(self: Self, key: [32]u8, nonce: [12]u8, plaintext: []const u8, ciphertext: []u8, tag: *[16]u8) !void {
        if (ciphertext.len != plaintext.len) return error.InvalidLength;

        switch (self.backend) {
            .native => {
                const cipher = std.crypto.aead.chacha_poly.ChaCha20Poly1305;
                cipher.encrypt(ciphertext, tag, plaintext, &[0]u8{}, nonce, key);
            },
            .shroud => {
                // Fallback to native
                const cipher = std.crypto.aead.chacha_poly.ChaCha20Poly1305;
                cipher.encrypt(ciphertext, tag, plaintext, &[0]u8{}, nonce, key);
            },
            .none => {
                @memcpy(ciphertext, plaintext); // No encryption
                @memset(tag, 0);
            },
        }
    }

    /// Symmetric decryption (ChaCha20-Poly1305)
    pub fn decrypt(self: Self, key: [32]u8, nonce: [12]u8, ciphertext: []const u8, tag: [16]u8, plaintext: []u8) !void {
        if (plaintext.len != ciphertext.len) return error.InvalidLength;

        switch (self.backend) {
            .native => {
                const cipher = std.crypto.aead.chacha_poly.ChaCha20Poly1305;
                try cipher.decrypt(plaintext, ciphertext, &[0]u8{}, tag, nonce, key);
            },
            .shroud => {
                // Fallback to native
                const cipher = std.crypto.aead.chacha_poly.ChaCha20Poly1305;
                try cipher.decrypt(plaintext, ciphertext, &[0]u8{}, tag, nonce, key);
            },
            .none => {
                @memcpy(plaintext, ciphertext); // No decryption
            },
        }
    }

    /// Check if post-quantum crypto is available
    pub fn hasPQCrypto(self: Self) bool {
        return switch (self.backend) {
            .shroud => self.config.enable_pq,
            else => false,
        };
    }

    /// Check if zero-knowledge proofs are available
    pub fn hasZKP(self: Self) bool {
        return switch (self.backend) {
            .shroud => self.config.enable_zkp,
            else => false,
        };
    }

    /// Post-quantum signature verification (using Ed25519 as classical fallback)
    pub fn verifyPQ(self: Self, message: []const u8, signature: []const u8, public_key: []const u8) !bool {
        _ = self;

        // Validate input sizes
        if (signature.len != 64) return error.InvalidSignatureLength;
        if (public_key.len != 32) return error.InvalidPublicKeyLength;

        // Use Ed25519 for classical verification
        const sig = std.crypto.sign.Ed25519.Signature{ .bytes = signature[0..64].* };
        const pubkey = std.crypto.sign.Ed25519.PublicKey{ .bytes = public_key[0..32].* };

        // Verify signature
        sig.verify(message, pubkey) catch return false;
        return true;
    }

    /// Post-quantum signing (using Ed25519 as classical fallback)
    pub fn signPQ(self: Self, message: []const u8, private_key: []const u8, allocator: std.mem.Allocator) ![]u8 {
        _ = self;

        // Validate input size
        if (private_key.len != 64) return error.InvalidPrivateKeyLength;

        // Use Ed25519 for classical signing
        const secret_key = std.crypto.sign.Ed25519.SecretKey{ .bytes = private_key[0..64].* };
        const signature = try secret_key.sign(message, null);

        // Return signature as allocated slice
        const sig_bytes = try allocator.alloc(u8, 64);
        @memcpy(sig_bytes, &signature.bytes);
        return sig_bytes;
    }
};

/// Feature detection for runtime configuration
pub fn detectAvailableFeatures() CryptoConfig {
    var config = CryptoConfig{};

    // Default to native std.crypto backend
    config.backend = .native;
    config.enable_pq = false;
    config.enable_zkp = false;

    return config;
}
