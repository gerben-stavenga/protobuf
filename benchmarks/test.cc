#include <benchmark/benchmark.h>

#include <string.h>
#include <x86intrin.h>

#include <string>
#include <random>

#include "google/protobuf/parse_context.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/json/json.h"

#include "benchmarks/test.pb.h"

__attribute__((noinline))
const char* Parse(const char* ptr, const char* end) {
    while (ptr < end) {
        uint32_t tag;
        ptr = google::protobuf::internal::ReadTag(ptr, &tag);
        switch (tag >> 3) {
            case 1: {
                if (tag != 8) return nullptr;
                uint64_t x;
                ptr = google::protobuf::internal::VarintParse(ptr, &x);
                break;
            }
            case 2: {
                if (tag != 16 + 2) return nullptr;
                uint32_t sz = google::protobuf::internal::ReadSize(&ptr);
                ptr += sz;
                break;
            }
            case 3: {
                if (tag != 24 + 1) return nullptr;
                ptr += 8;
                break;
            }
            case 4: {
                if (tag != 32 + 5) return nullptr;
                ptr += 4;
                break;
            }
            default:
                return nullptr;
        }
    }
    return ptr;
}

inline uint64_t L64(const char* ptr) {
    uint64_t x;
    std::memcpy(&x, ptr, 8);
    return x;
}

__attribute__((noinline))
const char* ParseTest(const char* ptr, const char* end) {
    uint32_t nexttag = *ptr;
    while (ptr < end) {
        uint32_t tag = nexttag;
        uint64_t data = L64(ptr);
        auto x = data | 0x7f7f7f7f7f7f7f7f;
        auto y = x ^ (x + 1);
        auto tagsize = __builtin_popcountll(y) / 8;
        auto fixedsize = tagsize + (tag & 4 ? 4 : 8); 
        x |= y;
        y = x ^ (x + 1);
        auto varintsz = __builtin_popcountll(y) / 8;
        auto nextptr = ptr + (tag & 1 ? fixedsize : varintsz);
        nexttag = *nextptr;
        asm volatile(""::"r"(nexttag));
        if (__builtin_expect((tag & 7) == 2, 0)) {
                ptr++;
                uint32_t sz = google::protobuf::internal::ReadSize(&ptr);
                ptr += sz;
                nexttag = *ptr;
        } else {
            ptr = nextptr;
        }
        /*switch (tag & 7) {
            case 0:
            case 1:
            case 5:
                ptr = nextptr;
                break;
            case 2: {
                ptr++;
                uint32_t sz = google::protobuf::internal::ReadSize(&ptr);
                ptr += sz;
                nexttag = *ptr;
                break;
            }
            case 3:
                asm volatile("");
            case 4:
                asm volatile("");
            case 6:
                asm volatile("");
                return nullptr;
            case 7:
                asm volatile("");
                return nullptr;
        }*/
    }
    return ptr;
}

const uint64_t entries[] = {
    0x0020000000020008,  // optional int32 varint = 1;
    0x0018000000010012,  // optional bytes bytes = 2;
    0x0028000000080119,  // optional fixed64 fixed64 = 3;
    0x0024000000040025,  // optional fixed32 fixed32 = 4;
};

struct Table {
    int offset_hasbits;
    int entry_cutoff;
    const uint64_t* entries;
};

const Table table = {
    16,
    4,
    entries
};

template <typename T>
T& RefAt(void* msg, ptrdiff_t offset) {
    return *reinterpret_cast<T*>(static_cast<char*>(msg) + offset);
}

__attribute__((noinline))
const char* ParseProto(const char* ptr, const char* end, google::protobuf::MessageLite* msg) {
    uint64_t hasbits = uint64_t(RefAt<uint32_t>(msg, table.offset_hasbits)) << 16;
    while (ptr < end) {
        uint32_t tag = uint8_t(*ptr);
        uint32_t field_num;
        if (tag & 0x80) {
            // TODO fix tags > 2 varint
            field_num = (tag - 0x80 + (uint32_t(uint8_t(ptr[1])) << 7)) >> 3;
            ptr += 2;
        } else {
            field_num = tag >> 3;
            ptr += 1;
        }
        uint64_t entry;
        if (field_num - 1 >= table.entry_cutoff) {
            return nullptr;
        } else {
            entry = table.entries[field_num - 1];
        }
        if (uint8_t(entry) != uint8_t(tag)) return nullptr;
        hasbits |= entry;
        if (__builtin_expect((tag & 7) == 2, 0)) {
            uint32_t sz = google::protobuf::internal::ReadSize(&ptr);
            auto offset = entry >> 48;
            RefAt<google::protobuf::internal::ArenaStringPtr>(msg, offset).SetBytes(ptr, sz, nullptr);
            ptr += sz;
        } else {
            static const uint64_t size_mask[] = {
                0xFF,
                0xFFFF,
                0xFFFFFF,
                0xFFFFFFFF,
                0xFFFFFFFFFF,
                0xFFFFFFFFFFFF,
                0xFFFFFFFFFFFFFF,
                0xFFFFFFFFFFFFFFFF
            };
            uint64_t data = L64(ptr);
            uint64_t mask = 0x7f7f7f7f7f7f7f7f;
            auto x = data | mask;
            auto y = x ^ (x + 1);
            auto varintsize = __builtin_popcountll(y) / 8;
            auto fixedsize = tag & 4 ? 4 : 8;
            if (tag & 1) mask = -1;
            auto size = (tag & 1 ? fixedsize : varintsize);
            data &= size_mask[size - 1];
            data = _pext_u64(data, mask);
            auto offset = entry >> 48;
            RefAt<uint32_t>(msg, offset + ((entry & 0x100) ? 4 : 0)) = data >> 32;
            RefAt<uint32_t>(msg, offset) = data;
            ptr += size;
        }
    }
    RefAt<uint32_t>(msg, table.offset_hasbits) = hasbits >> 16;
    return ptr;
}


void WriteRandom(std::string* s) {
    google::protobuf::io::StringOutputStream os(s);
    google::protobuf::io::CodedOutputStream out(&os);
    std::mt19937 gen(0x3523fa4f);
    std::uniform_int_distribution pick(1, 4);
    for (int i = 0; i < 10000; i++) {
again:
        auto tag = pick(gen);
        switch (tag) {
            case 1:
                out.WriteTag(8 + 0);
                out.WriteVarint32(20);
                break;
            case 2:
                goto again;
                out.WriteTag(16 + 2);
                out.WriteVarint32(5);
                out.WriteString("Hello");
                break;
            case 3:
                out.WriteTag(24 + 1);
                out.WriteLittleEndian64(0xdeadbeef);
                break;
            case 4:
                out.WriteTag(32 + 5);
                out.WriteLittleEndian32(0xdead);
                break;
            default:
                exit(-1);
        }
    }
}

static void BM_RegularParse(benchmark::State& state) {
    std::string x;
    WriteRandom(&x);
    for (auto _ : state) {
        if (Parse(x.data(), x.data() + x.size()) == nullptr) exit(-1);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK(BM_RegularParse);

static void BM_NewParse(benchmark::State& state) {
    std::string x;
    WriteRandom(&x);
    for (auto _ : state) {
        if (ParseTest(x.data(), x.data() + x.size()) == nullptr) exit(-1);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK(BM_NewParse);

static void BM_Proto2Parse(benchmark::State& state) {
    std::string x;
    WriteRandom(&x);
    test_benchmark::TestProto proto;
    for (auto _ : state) {
        proto.ParseFromString(x);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK(BM_Proto2Parse);

static void BM_TableParse(benchmark::State& state) {
    std::string x;
    WriteRandom(&x);
    test_benchmark::TestProto proto;
    for (auto _ : state) {
        ParseProto(x.data(), x.data() + x.size(), &proto);
    }
    /*std::string s;
    google::protobuf::json::MessageToJsonString(proto, &s);
    std::cout << s;*/
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK(BM_TableParse);
