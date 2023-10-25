#include <benchmark/benchmark.h>

#include <string.h>
#ifdef __X86_64__
#include <x86intrin.h>
#endif

#include <string>
#include <random>

#include "google/protobuf/parse_context.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/json/json.h"

#include "benchmarks/test.pb.h"
#include "benchmarks/test.upb.h"

namespace google {
namespace protobuf {
namespace internal {

__attribute__((noinline))
const char* Parse(const char* ptr, const char* end) {
    while (ptr < end) {
        uint32_t tag;
        ptr = ReadTag(ptr, &tag);
        if ((tag & 7) == 4) break;
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
            case 5: {
                if (tag != 40 + 2) return nullptr;
                uint32_t sz = ReadSize(&ptr);
                ptr = Parse(ptr, ptr + sz);
                break;
            }
            case 6: {
                if (tag != 48 + 3) return nullptr;
                ptr = Parse(ptr, end);
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
    int start_field_num;
    int entry_cutoff;
    const uint64_t* entries;
};

const uint64_t test_proto_sub_group_entries[] = {
    0x0018000000010008 | kZigZag,  // optional sint32 varint = 1;
};

const ParseTable test_proto_sub_group_parse_table = {
    &test_benchmark::_TestProto_SubGroup_default_instance_,
    nullptr,
    16,
    1,
    1,
    test_proto_sub_group_entries,
};

const uint64_t test_proto_entries[] = {
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
    1,
    6,
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

Utf8Checker utf8_checker;

__attribute__((noinline))
const char* ParseProto(MessageLite* msg, const char* ptr, ParseContext* ctx, const ParseTable* table, int delta_or_group_num) {
    //if (--ctx->depth_ == 0) return nullptr;

    #define ERROR do { if (1) std::cout << __LINE__ << " failure\n"; goto error; } while (0)

    uint64_t hasbits = 0;
    if (table->offset_hasbits) hasbits = uint64_t(RefAt<uint32_t>(msg, table->offset_hasbits)) << 16;
    while (!ctx->Done(&ptr)) {
        uint32_t tag = uint8_t(*ptr);
        uint32_t field_num;
        ptr = ReadTagInlined(ptr, &field_num);
        if (ptr == nullptr) ERROR;
        field_num >>= 3;
        if (ABSL_PREDICT_FALSE((tag & 7) == 4)) {
            if (field_num != -delta_or_group_num) ERROR;
            goto group_end;
        }
        uint64_t entry;
        if (ABSL_PREDICT_FALSE(field_num - table->start_field_num >= table->entry_cutoff)) {
            if (tag == 0) {
                goto group_end;
            }
            // TODO search in sparse list
            goto unknown_field;
        } else {
            entry = table->entries[field_num - table->start_field_num];
        }
        asm("":: "r"(entry));
        if (uint8_t(entry) != uint8_t(tag)) {
unknown_field:
            // TODO unknown field
            ERROR;
        }
        int delta;
        if (ABSL_PREDICT_FALSE((tag & 7) == 2)) {
            uint32_t sz = ReadSize(&ptr);
            if (ptr == nullptr) ERROR;
            if (ABSL_PREDICT_TRUE((entry & kMessage) == 0)) {
                auto offset = entry >> 48;
                RefAt<ArenaStringPtr>(msg, offset).SetBytes(ptr, sz, nullptr);
                utf8_checker.BatchTest(RefAt<ArenaStringPtr>(msg, offset).Get(), entry & kCheckUtf8);
                ptr += sz;
            } else {
                delta = ctx->PushLimit(ptr, sz).token();
                goto parse_submsg;
            }
            if (ABSL_PREDICT_FALSE(entry & (kExcessHasbits | kCardinality))) {
                // TODO
                ERROR;
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
            if (ABSL_PREDICT_FALSE((tag & 7) == 3)) {
                delta = -field_num;
parse_submsg:
                auto aux_idx = entry >> 48;
                unsigned offset = table->aux[aux_idx].offset;
                const ParseTable* child_table = static_cast<const ParseTable*>(table->aux[aux_idx].table);
                MessageLite*& child = RefAt<MessageLite*>(msg, offset);
                if (child == nullptr) {
                    child = static_cast<const MessageLite*>(child_table->default_instance)->New();
                }
                ptr = ParseProto(child, ptr, ctx, child_table, delta);
                if (ptr == nullptr) ERROR;
                continue;
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
#ifdef __X86_64__
            data = _pext_u64(data, mask);
#else
            // TODO find good sequence for arm
#endif
            if (entry & kZigZag) data = WireFormatLite::ZigZagDecode64(data);
            if (ABSL_PREDICT_FALSE(entry & (kBool | kExcessHasbits | kCardinality))) {
                // TODO
                ERROR;
            } else {
                hasbits |= entry;
            }
            auto offset = entry >> 48;
            RefAt<uint32_t>(msg, offset + ((entry & k64bit) ? 4 : 0)) = data >> 32;
            RefAt<uint32_t>(msg, offset) = data;
            ptr += size;
        }
    }
    if (delta_or_group_num < 0) ERROR;
    if (delta_or_group_num > 0) {
        if (!ctx->PopLimit(EpsCopyInputStream::LimitToken(delta_or_group_num))) ERROR;
    }
group_end:
    // Sync hasbits
    if (table->offset_hasbits) RefAt<uint32_t>(msg, table->offset_hasbits) = hasbits >> 16;
    // ctx->depth_++;
    return ptr;
error:
    ptr = nullptr;
    goto group_end;
}


void WriteRandom(std::string* s, int iters, int level) {
    io::StringOutputStream os(s);
    io::CodedOutputStream out(&os);
    std::mt19937 gen(0x3523fa4f);
    std::uniform_int_distribution pick(1, 6);
    for (int i = 0; i < iters; i++) {
again:
        auto tag = pick(gen);
        switch (tag) {
            case 1:
                out.WriteTag(tag * 8 + 0);
                out.WriteVarint32(20);
                break;
            case 2:
                if (level < 1) goto again;
                out.WriteTag(tag * 8 + 2);
                out.WriteVarint32(5);
                out.WriteString("Hello");
                break;
            case 3:
                out.WriteTag(tag * 8 + 1);
                out.WriteLittleEndian64(0xdeadbeef);
                break;
            case 4:
                out.WriteTag(tag * 8 + 5);
                out.WriteLittleEndian32(0xdead);
                break;
            case 5:
                if (level < 2) goto again;
                out.WriteTag(tag * 8 + 2);
                out.WriteVarint32(2);
                out.WriteTag(8 + 0);
                out.WriteVarint32(1);
                break;
            case 6:
                if (level < 2) goto again;
                out.WriteTag(tag * 8 + 3);
                out.WriteTag(8 + 0);
                out.WriteVarint32(WireFormatLite::ZigZagEncode32(-30));
                out.WriteTag(tag * 8 + 4);
                break;
            default:
                exit(-1);
        }
    }
}

static void BM_RegularParse(benchmark::State& state) {
    std::string x;
    WriteRandom(&x, state.range(0), 2);
    for (auto _ : state) {
        if (Parse(x.data(), x.data() + x.size()) == nullptr) exit(-1);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK(BM_RegularParse)->Range(1024, 256 * 1024)->RangeMultiplier(4);

#if 0
static void BM_NewParse(benchmark::State& state) {
    std::string x;
    WriteRandom(&x, state.range(0), true);
    for (auto _ : state) {
        if (ParseTest(x.data(), x.data() + x.size()) == nullptr) exit(-1);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK(BM_NewParse)->Range(1024, 256 * 1024)->RangeMultiplier(4);
#endif

static void BM_Proto2Parse(benchmark::State& state, int level) {
    std::string x;
    WriteRandom(&x, state.range(0), level);
    test_benchmark::TestProto proto;
    for (auto _ : state) {
        proto.ParseFromString(x);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK_CAPTURE(BM_Proto2Parse, nostring, 0)->Range(1024, 256 * 1024)->RangeMultiplier(4);
BENCHMARK_CAPTURE(BM_Proto2Parse, string, 1)->Range(1024, 256 * 1024)->RangeMultiplier(4);
BENCHMARK_CAPTURE(BM_Proto2Parse, submsg, 2)->Range(1024, 256 * 1024)->RangeMultiplier(4);

#if 0

int64_t buf[8191];

static void BM_UpbParse(benchmark::State& state, int level) {
    std::string x;
    WriteRandom(&x, state.range(0), level);
    test_benchmark::TestProto proto;
    for (auto _ : state) {
        upb_Arena* arena = upb_Arena_Init(buf, sizeof(buf), nullptr);
        auto* proto =
            test_benchmark_TestProto_parse_ex(x.data(), x.size(), nullptr, 0, arena);
        if (!proto) {
            printf("Failed to parse.\n");
            exit(1);
        }
        upb_Arena_Free(arena);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK_CAPTURE(BM_UpbParse, nostring, 0);
BENCHMARK_CAPTURE(BM_UpbParse, string, 1);
BENCHMARK_CAPTURE(BM_UpbParse, submsg, 2);

#endif

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
    ptr = ParseProto(&proto, ptr, &ctx, &test_proto_parse_table, 0);
    if (ptr == nullptr) {
        std::cout << "Parse fail\n";
        exit(-1);
    }
    std::string s;
    json::PrintOptions print_options{true};
    json::MessageToJsonString(proto, &s, print_options).IgnoreError();
    std::cout << s;
}

static void BM_TableParse(benchmark::State& state, int level) {
    std::string x;
    WriteRandom(&x, state.range(0), level);
    test_benchmark::TestProto proto;
    for (auto _ : state) {
        const char* ptr;
        ParseContext ctx(20, false, &ptr, x);
        ParseProto(&proto, ptr, &ctx, &test_proto_parse_table, 0);
    }
    state.SetBytesProcessed(state.iterations() * x.size());
}
BENCHMARK_CAPTURE(BM_TableParse, nostring, 0)->Range(1024, 256 * 1024)->RangeMultiplier(4);
BENCHMARK_CAPTURE(BM_TableParse, string, 1)->Range(1024, 256 * 1024)->RangeMultiplier(4);
BENCHMARK_CAPTURE(BM_TableParse, submsg, 2)->Range(1024, 256 * 1024)->RangeMultiplier(4);


}  // namespace internal
}  // namespace protobuf
}  // namespace google
