#include <iostream>
#include <string>
#include <cstdint>

class BitWriter {
private:
    int bitShift; // bit offset from the right inside the current long
    std::string& buffer;
    uint64_t b; // current element

    // Helper function to perform the actual bit writing
    void doWrite(uint32_t x, int numBits) {
        bitShift -= numBits;
        uint64_t mask = (1ULL << numBits) - 1;
        b |= (static_cast<uint64_t>(x) & mask) << bitShift;
    }

public:
    // Constructor
    BitWriter(std::string& buf) : buffer(buf), bitShift(64), b(0) {}

    // Method to write the least significant numBits of x into the BitBuffer
    void write(uint32_t x, int numBits) {
        int k = numBits - bitShift;
        if (k < 0) {
            doWrite(x, numBits);
        } else {
            doWrite(x >> k, bitShift);
            if (k > 0) {
                bitShift = 64;
                flush();
                doWrite(x, k);
            }
        }
    }

    // Method to flush the temporary long to the buffer
    void flush() {
        if (bitShift != 64) {
            for (int i = 7; i >= 0; --i) {
                buffer.push_back(static_cast<char>((b >> (i * 8)) & 0xFF));
            }
            b = 0;
            bitShift = 64;
        }
    }
};

class BitReader {
private:
    int bitShift; // bit offset from the right inside the current long
    uint64_t b; // current element
    const std::string& buffer;
    size_t bufferIndex; // current index in the buffer

    // Helper function to perform the actual bit reading
    uint64_t doRead(int numBits) {
        bitShift -= numBits;
        uint64_t mask = (1ULL << numBits) - 1;
        return (b >> bitShift) & mask;
    }

    // Method to get the next long from the buffer
    uint64_t getNextLong() {
        if (bufferIndex + 8 > buffer.size()) {
            throw std::out_of_range("Buffer does not contain enough data for next long");
        }
        uint64_t nextLong = 0;
        for (int i = 0; i < 8; ++i) {
            nextLong = (nextLong << 8) | static_cast<unsigned char>(buffer[bufferIndex++]);
        }
        return nextLong;
    }

public:
    // Constructor
    BitReader(const std::string& buf) : buffer(buf), bitShift(64), bufferIndex(0) {
        b = getNextLong();
    }

    // Method to read the specified number of bits as a long
    uint64_t readLong(int numBits) {
        if (numBits > 64 || numBits < 0) {
            throw std::invalid_argument("Number of bits to read must be between 0 and 64");
        }
        
        int k = numBits - bitShift;
        if (k < 0) {
            return doRead(numBits);
        } else {
            uint64_t x = doRead(bitShift) << k;
            if (k > 0) {
                bitShift = 64;
                b = getNextLong();
                x |= doRead(k);
            }
            return x;
        }
    }

    // Method to read the specified number of bits as an int
    uint32_t read(int numBits) {
        return static_cast<uint32_t>(readLong(numBits));
    }
};
