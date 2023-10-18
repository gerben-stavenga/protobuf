#include <benchmark/benchmark.h>

#include <string.h>

#include <string>
#include <random>

#include "google/protobuf/parse_context.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"

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

void WriteRandom(std::string* s) {
    google::protobuf::io::StringOutputStream os(s);
    google::protobuf::io::CodedOutputStream out(&os);
    std::mt19937 gen(0x3523fa4f);
    std::uniform_int_distribution pick(1, 4);
    for (int i = 0; i < 3000; i++) {
        auto tag = pick(gen);
        switch (tag) {
            case 1:
                out.WriteTag(8 + 0);
                out.WriteVarint32(20);
                break;
            case 2:
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
    state.SetBytesProcessed(x.size());
}
BENCHMARK(BM_RegularParse);
