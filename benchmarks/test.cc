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

namespace google {
namespace protobuf {
namespace internal {

__attribute__((noinline))
const char* Parse(const char* ptr, const char* end) {
    while (ptr < end) {
        uint32_t tag;
        ptr = ReadTag(ptr, &tag);
        switch (tag >> 3) {
            case 1: {
                if (tag != 8) return nullptr;
                uint64_t x;
                ptr = VarintParse(ptr, &x);
                break;
            }
            case 2: {
                if (tag != 16 + 2) return nullptr;
                uint32_t sz = ReadSize(&ptr);
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
                uint32_t sz = ReadSize(&ptr);
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
                uint32_t sz = ReadSize(&ptr);
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

enum {
    // singular = 0, repeated = 1, oneof = 2
    kCardinality = 0x300,
    kExcessHasbits = 0x2000,
    // Primitives
    k64bit = 0x400,
    kZigZag = 0x800,
    kBool = 0x1000,
    // Length delimited
    // string = 0, message = 1, map = 2, cord, stringview, packed bool, uint32, sint32, uint64, sint64, f32, f64
    kMessage = 0x400,
    kCheckUtf8 = 0x800,
};

struct AuxData {
    uint32_t offset;
    uint32_t oneof;
    const void* table;
};

struct ParseTable {
    const void* default_instance;
    const AuxData* aux;
    int offset_hasbits;
    int entry_cutoff;
    const uint64_t* entries;
};

const uint64_t test_proto_sub_group_entries[] = {
    0x7,  //
    0x0018000000010008 | kZigZag,  // optional sint32 varint = 1;
};

const ParseTable test_proto_sub_group_parse_table = {
    &test_benchmark::_TestProto_SubGroup_default_instance_,
    nullptr,
    16,
    2,
    test_proto_sub_group_entries,
};

const uint64_t test_proto_entries[] = {
    0x7,  // illegal tag which forces fieldnum to go into tag guard 0 != 7
    0x0030000000080008,  // optional int32 varint = 1;
    0x0018000000010012,  // optional bytes bytes = 2;
    0x0038000000200019 | k64bit,  // optional fixed64 fixed64 = 3;
    0x0034000000100025,  // optional fixed32 fixed32 = 4;
    0x000000000002002A | kMessage,  // optional TestProto recurse = 5;
    0x0001000000040033,  // optional group sub_group = 6;
};

extern const ParseTable test_proto_parse_table;
const AuxData test_proto_aux[] = {
    {0x20, 0, &test_proto_parse_table},
    {0x28, 0, &test_proto_sub_group_parse_table},
};

const ParseTable test_proto_parse_table = {
    &test_benchmark::_TestProto_default_instance_,
    test_proto_aux,
    16,
    7,
    test_proto_entries
};

template <typename T>
T& RefAt(void* msg, ptrdiff_t offset) {
    return *reinterpret_cast<T*>(static_cast<char*>(msg) + offset);
}

class Utf8Checker {
public:
    bool BatchTest(absl::string_view s, bool do_test) {
        utf8_tests[utf8_idx] = s;
        utf8_idx += int(do_test);
        if (ABSL_PREDICT_FALSE(utf8_idx == kNumUtf8Tests)) {
            return DoTests();
        }
        return true;
    }

    bool DoTests() {
        for (unsigned i = 0; i < utf8_idx; i++) if (!VerifyUTF8(utf8_tests[i], "")) return false;
        utf8_idx = 0;
        return true;
    }

private:
    static constexpr unsigned kNumUtf8Tests = 10;
    absl::string_view utf8_tests[kNumUtf8Tests];
    unsigned utf8_idx = 0;
};

__attribute__((noinline))
const char* ParseProto(MessageLite* msg, const char* ptr, ParseContext* ctx, const ParseTable* table) {
    //if (--ctx->depth_ == 0) return nullptr;

    #define FAIL_AND_BREAK if (true) { if (0) std::cout << __LINE__ << " error\n"; ptr = nullptr; break; } else (void)0

    Utf8Checker utf8_checker;
    uint64_t hasbits = 0;
    if (table->offset_hasbits) hasbits = uint64_t(RefAt<uint32_t>(msg, table->offset_hasbits)) << 16;
    while (!ctx->Done(&ptr)) {
        uint32_t tag = uint8_t(*ptr);
        uint32_t field_num;
        ptr = ReadTagInlined(ptr, &field_num);
        if (ptr == nullptr) break;
        field_num >>= 3;
        uint64_t entry;
        if (ABSL_PREDICT_FALSE(field_num >= table->entry_cutoff)) {
            if ((tag & 7) == 4) goto end_group;
            // TODO 
            FAIL_AND_BREAK;
        } else {
            entry = table->entries[field_num];
        }
        if (uint8_t(entry) != uint8_t(tag)) {
            if (tag == 0) break;
            // TODO unknown field
            FAIL_AND_BREAK;
        }
        if ((tag & 7) == 2) {
            if (ABSL_PREDICT_TRUE((entry & kMessage) == 0)) {
                uint32_t sz = ReadSize(&ptr);
                if (ptr == nullptr) break;
                auto offset = entry >> 48;
                RefAt<ArenaStringPtr>(msg, offset).SetBytes(ptr, sz, nullptr);
                utf8_checker.BatchTest(RefAt<ArenaStringPtr>(msg, offset).Get(), entry & kCheckUtf8);
                ptr += sz;
            } else {
                uint32_t sz = ReadSize(&ptr);
                if (ptr == nullptr) break;
                auto aux_idx = entry >> 48;
                unsigned offset = table->aux[aux_idx].offset;
                const ParseTable* child_table = static_cast<const ParseTable*>(table->aux[aux_idx].table);
                MessageLite*& child = RefAt<MessageLite*>(msg, offset);
                if (child == nullptr) {
                    child = static_cast<const MessageLite*>(child_table->default_instance)->New();
                }
                auto delta = ctx->PushLimit(ptr, sz);
                ptr = ParseProto(child, ptr, ctx, child_table);
                if (ptr == nullptr) break;
                if (!ctx->PopLimit(std::move(delta))) FAIL_AND_BREAK;
            }
            if (ABSL_PREDICT_FALSE(entry & (kExcessHasbits | kCardinality))) {
                // TODO
                FAIL_AND_BREAK;
            } else {
                hasbits |= entry;
            }
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
            if (ABSL_PREDICT_FALSE((1 << (tag & 7)) & 0b11011000)) {
                if ((tag & 7) == 3) {
                    auto aux_idx = entry >> 48;
                    unsigned offset = table->aux[aux_idx].offset;
                    const ParseTable* child_table = static_cast<const ParseTable*>(table->aux[aux_idx].table);
                    MessageLite*& child = RefAt<MessageLite*>(msg, offset);
                    if (child == nullptr) {
                        child = static_cast<const MessageLite*>(child_table->default_instance)->New();
                    }
                    if (ABSL_PREDICT_FALSE(entry & (kBool | kExcessHasbits | kCardinality))) {
                        // TODO
                        FAIL_AND_BREAK;
                    } else {
                        hasbits |= entry;
                    }
                    ptr = ParseProto(child, ptr, ctx, child_table);
                    if (ptr == nullptr) break;
                    if (ctx->LastTag() != field_num) FAIL_AND_BREAK;
                    continue;
                } else if ((tag & 7) == 4) {
                    end_group:
                    ctx->SetLastTag(field_num);
                    break;
                }
                FAIL_AND_BREAK;
            }
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
            if (entry & kZigZag) data = WireFormatLite::ZigZagDecode64(data);
            if (ABSL_PREDICT_FALSE(entry & (kBool | kExcessHasbits | kCardinality))) {
                // TODO
                FAIL_AND_BREAK;
            } else {
                hasbits |= entry;
            }
            auto offset = entry >> 48;
            RefAt<uint32_t>(msg, offset + ((entry & k64bit) ? 4 : 0)) = data >> 32;
            RefAt<uint32_t>(msg, offset) = data;
            ptr += size;
        }
    }
    // Sync hasbits
    if (table->offset_hasbits) RefAt<uint32_t>(msg, table->offset_hasbits) = hasbits >> 16;
    if (!utf8_checker.DoTests()) return nullptr;
    // ctx->depth_++;
    return ptr;
}


void WriteRandom(std::string* s, int level) {
    io::StringOutputStream os(s);
    io::CodedOutputStream out(&os);
    std::mt19937 gen(0x3523fa4f);
    std::uniform_int_distribution pick(1, 6);
    for (int i = 0; i < 10000; i++) {
again:
        auto tag = pick(gen);
        switch (tag) {
            case 1:
                out.WriteTag(8 + 0);
                out.WriteVarint32(20);
                break;
            case 2:
                if (level < 1) goto again;
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
            case 5:
                if (level < 2) goto again;
                out.WriteTag(40 + 2);
                out.WriteVarint32(2);
                out.WriteTag(8 + 0);
                out.WriteVarint32(1);
                break;
            case 6:
                if (level < 2) goto again;
                out.WriteTag(48 + 3);
                out.WriteTag(8 + 0);
                out.WriteVarint32(WireFormatLite::ZigZagEncode32(-30));
                out.WriteTag(48 + 4);
                break;
            default:
                exit(-1);
        }
    }
}

static void BM_RegularParse(benchmark::State& state) {
    std::string x;
    WriteRandom(&x, true);
    for (auto _ : state) {
        if (Parse(x.data(), x.data() + x.size()) == nullptr) exit(-1);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK(BM_RegularParse);

static void BM_NewParse(benchmark::State& state) {
    std::string x;
    WriteRandom(&x, true);
    for (auto _ : state) {
        if (ParseTest(x.data(), x.data() + x.size()) == nullptr) exit(-1);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK(BM_NewParse);

static void BM_Proto2Parse(benchmark::State& state, bool nostring) {
    std::string x;
    WriteRandom(&x, nostring);
    test_benchmark::TestProto proto;
    for (auto _ : state) {
        proto.ParseFromString(x);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK_CAPTURE(BM_Proto2Parse, nostring, 0);
BENCHMARK_CAPTURE(BM_Proto2Parse, string, 1);
BENCHMARK_CAPTURE(BM_Proto2Parse, submsg, 2);

void TestParse() {
    test_benchmark::TestProto proto;
    proto.set_varint(10);
    proto.set_bytes("Cool");
    proto.set_fixed64(64);
    proto.set_fixed32(32);
    {
        auto child = proto.mutable_recurse();
        child->set_varint(1000);
        child->set_bytes("CoolCool");
        child->set_fixed64(640);
        child->set_fixed32(320);
    }
    {
        auto child = proto.mutable_subgroup();
        child->set_varint(-20);
    }
    auto serialized = proto.SerializeAsString();
    proto.Clear();
    const char* ptr;
    ParseContext ctx(20, false, &ptr, serialized);
    ptr = ParseProto(&proto, ptr, &ctx, &test_proto_parse_table);
    if (ptr == nullptr) {
        std::cout << "Parse fail\n";
        exit(-1);
    }
    std::string s;
    json::PrintOptions print_options{true};
    json::MessageToJsonString(proto, &s, print_options).IgnoreError();
    std::cout << s;
}

static void BM_TableParse(benchmark::State& state, bool nostring) {
    std::string x;
    WriteRandom(&x, nostring);
    test_benchmark::TestProto proto;
    for (auto _ : state) {
        const char* ptr;
        ParseContext ctx(20, false, &ptr, x);
        ParseProto(&proto, ptr, &ctx, &test_proto_parse_table);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK_CAPTURE(BM_TableParse, nostring, 0);
BENCHMARK_CAPTURE(BM_TableParse, string, 1);
BENCHMARK_CAPTURE(BM_TableParse, submsg, 2);


}  // namespace internal
}  // namespace protobuf
}  // namespace google
