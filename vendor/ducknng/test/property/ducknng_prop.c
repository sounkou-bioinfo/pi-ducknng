#include "duckdb_extension.h"
DUCKDB_EXTENSION_GLOBAL
#undef duckdb_malloc
#undef duckdb_free
#undef duckdb_vector_size

#include "ducknng_quack.h"
#include "ducknng_quack_core.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include "ducknng_wire.h"

#include "greatest.h"
#include "theft.h"

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DUCKNNG_PROP_DEFAULT_TRIALS 1000u
#define DUCKNNG_PROP_DEFAULT_SEED UINT64_C(0xd17c0ffee1234567)
#define DUCKNNG_PROP_MAX_RANDOM_BYTES 512u
#define DUCKNNG_PROP_MAX_RANDOM_URL 256u
#define DUCKNNG_PROP_MAX_FRAME_PAYLOAD 256u
#define DUCKNNG_PROP_MAX_FRAME_ERROR 32u

struct prop_bytes {
    size_t len;
    uint8_t data[];
};

struct prop_bytes_env {
    size_t max_len;
};

struct prop_frame {
    size_t len;
    uint8_t type;
    uint8_t status;
    uint32_t flags;
    uint32_t name_len;
    uint32_t error_len;
    uint64_t payload_len;
    uint8_t data[];
};

static void
prop_init_duckdb_api(void)
{
    duckdb_ext_api.duckdb_malloc = malloc;
    duckdb_ext_api.duckdb_free = free;
}

static size_t
prop_env_size(const char *name, size_t fallback)
{
    const char *value = getenv(name);
    char *end = NULL;
    unsigned long long parsed;

    if (!value || !value[0]) return fallback;
    errno = 0;
    parsed = strtoull(value, &end, 0);
    if (errno != 0 || !end || *end != '\0' || parsed == 0) return fallback;
    return (size_t)parsed;
}

static theft_seed
prop_env_seed(void)
{
    const char *value = getenv("DUCKNNG_PROP_SEED");
    char *end = NULL;
    unsigned long long parsed;

    if (!value || !value[0]) return (theft_seed)DUCKNNG_PROP_DEFAULT_SEED;
    errno = 0;
    parsed = strtoull(value, &end, 0);
    if (errno != 0 || !end || *end != '\0') return (theft_seed)DUCKNNG_PROP_DEFAULT_SEED;
    return (theft_seed)parsed;
}

static int
prop_env_bool(const char *name)
{
    const char *value = getenv(name);

    if (!value || !value[0]) return 0;
    return strcmp(value, "0") != 0 && strcmp(value, "false") != 0 &&
        strcmp(value, "FALSE") != 0 && strcmp(value, "no") != 0 &&
        strcmp(value, "NO") != 0;
}

static void
prop_write_le32(uint8_t *p, uint32_t value)
{
    p[0] = (uint8_t)(value & 0xffu);
    p[1] = (uint8_t)((value >> 8) & 0xffu);
    p[2] = (uint8_t)((value >> 16) & 0xffu);
    p[3] = (uint8_t)((value >> 24) & 0xffu);
}

static void
prop_write_le64(uint8_t *p, uint64_t value)
{
    prop_write_le32(p, (uint32_t)(value & UINT64_C(0xffffffff)));
    prop_write_le32(p + 4, (uint32_t)((value >> 32) & UINT64_C(0xffffffff)));
}

static uint64_t
prop_random_bounded(struct theft *t, uint64_t limit)
{
    uint64_t value;

    if (limit <= 1) return 0;
    value = theft_random_bits(t, 16);
    if (limit > UINT64_C(65536)) {
        value |= theft_random_bits(t, 16) << 16;
    }
    return value % limit;
}

static enum theft_alloc_res
prop_bytes_alloc(struct theft *t, void *env, void **instance)
{
    const struct prop_bytes_env *cfg = (const struct prop_bytes_env *)env;
    size_t max_len = cfg && cfg->max_len ? cfg->max_len : DUCKNNG_PROP_MAX_RANDOM_BYTES;
    size_t len = (size_t)prop_random_bounded(t, (uint64_t)max_len + 1u);
    struct prop_bytes *out;
    size_t i;

    out = (struct prop_bytes *)malloc(sizeof(*out) + len);
    if (!out) return THEFT_ALLOC_ERROR;
    out->len = len;
    for (i = 0; i < len; i++) {
        out->data[i] = (uint8_t)theft_random_bits(t, 8);
    }
    *instance = out;
    return THEFT_ALLOC_OK;
}

static void
prop_bytes_free(void *instance, void *env)
{
    (void)env;
    free(instance);
}

static void
prop_hexdump(FILE *f, const uint8_t *data, size_t len)
{
    size_t row;

    fprintf(f, "len=%zu\n", len);
    for (row = 0; row < len; row += 16) {
        size_t rem = len - row;
        size_t i;

        if (rem > 16) rem = 16;
        fprintf(f, "%04zx:", row);
        for (i = 0; i < rem; i++) fprintf(f, " %02x", data[row + i]);
        fprintf(f, "\n");
    }
}

static void
prop_bytes_print(FILE *f, const void *instance, void *env)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)instance;

    (void)env;
    prop_hexdump(f, bytes->data, bytes->len);
}

static struct prop_bytes_env prop_random_bytes_env = {
    .max_len = DUCKNNG_PROP_MAX_RANDOM_BYTES,
};

static struct prop_bytes_env prop_random_url_env = {
    .max_len = DUCKNNG_PROP_MAX_RANDOM_URL,
};

static struct theft_type_info prop_random_bytes_info = {
    .alloc = prop_bytes_alloc,
    .free = prop_bytes_free,
    .print = prop_bytes_print,
    .autoshrink_config = {
        .enable = true,
    },
    .env = &prop_random_bytes_env,
};

static struct theft_type_info prop_random_url_info = {
    .alloc = prop_bytes_alloc,
    .free = prop_bytes_free,
    .print = prop_bytes_print,
    .autoshrink_config = {
        .enable = true,
    },
    .env = &prop_random_url_env,
};

static enum theft_alloc_res
prop_frame_alloc(struct theft *t, void *env, void **instance)
{
    uint8_t type = (uint8_t)prop_random_bounded(t, 5);
    uint8_t status = type == DUCKNNG_RPC_ERROR
        ? (uint8_t)prop_random_bounded(t, DUCKNNG_STATUS_DISABLED + 1u) : 0;
    uint32_t flags = (uint32_t)theft_random_bits(t, 16) & DUCKNNG_RPC_FLAGS_MASK;
    uint32_t name_len = (uint32_t)prop_random_bounded(t, DUCKNNG_MAX_METHOD_NAME_LEN + 1u);
    uint32_t error_len = type == DUCKNNG_RPC_ERROR
        ? 1u + (uint32_t)prop_random_bounded(t, DUCKNNG_PROP_MAX_FRAME_ERROR)
        : 0u;
    uint64_t payload_len = prop_random_bounded(t, DUCKNNG_PROP_MAX_FRAME_PAYLOAD + 1u);
    size_t total;
    struct prop_frame *out;
    size_t i;

    (void)env;
    total = DUCKNNG_WIRE_HEADER_LEN + (size_t)name_len + (size_t)error_len + (size_t)payload_len;
    out = (struct prop_frame *)malloc(sizeof(*out) + total);
    if (!out) return THEFT_ALLOC_ERROR;
    out->len = total;
    out->type = type;
    out->status = status ? status :
        (type == DUCKNNG_RPC_ERROR ? DUCKNNG_STATUS_UNSPECIFIED : DUCKNNG_STATUS_OK);
    out->flags = flags;
    out->name_len = name_len;
    out->error_len = error_len;
    out->payload_len = payload_len;
    out->data[0] = DUCKNNG_WIRE_VERSION;
    out->data[1] = type;
    prop_write_le32(out->data + 2,
        flags | ((uint32_t)status << DUCKNNG_RPC_STATUS_SHIFT));
    prop_write_le32(out->data + 6, name_len);
    prop_write_le32(out->data + 10, error_len);
    prop_write_le64(out->data + 14, payload_len);
    for (i = DUCKNNG_WIRE_HEADER_LEN; i < total; i++) {
        out->data[i] = (uint8_t)theft_random_bits(t, 8);
    }
    *instance = out;
    return THEFT_ALLOC_OK;
}

static void
prop_frame_free(void *instance, void *env)
{
    (void)env;
    free(instance);
}

static void
prop_frame_print(FILE *f, const void *instance, void *env)
{
    const struct prop_frame *frame = (const struct prop_frame *)instance;

    (void)env;
    fprintf(f, "type=%u status=%u flags=%" PRIu32 " name_len=%" PRIu32
        " error_len=%" PRIu32 " payload_len=%" PRIu64 "\n",
        (unsigned int)frame->type, (unsigned int)frame->status,
        frame->flags, frame->name_len,
        frame->error_len, frame->payload_len);
    prop_hexdump(f, frame->data, frame->len);
}

static struct theft_type_info prop_frame_info = {
    .alloc = prop_frame_alloc,
    .free = prop_frame_free,
    .print = prop_frame_print,
    .autoshrink_config = {
        .enable = true,
    },
};

struct prop_two_sizes {
    uint64_t a;
    uint64_t b;
};

static enum theft_alloc_res
prop_two_sizes_alloc(struct theft *t, void *env, void **instance)
{
    struct prop_two_sizes *out;

    (void)env;
    out = (struct prop_two_sizes *)malloc(sizeof(*out));
    if (!out) return THEFT_ALLOC_ERROR;
    out->a = ((uint64_t)theft_random_bits(t, 32) << 32) | (uint64_t)theft_random_bits(t, 32);
    out->b = ((uint64_t)theft_random_bits(t, 32) << 32) | (uint64_t)theft_random_bits(t, 32);
    /* Bias toward boundary values so the overflow branches are hit often rather
     * than only on a rare random near-SIZE_MAX draw. */
    switch (theft_random_bits(t, 3)) {
    case 0: out->a = (uint64_t)SIZE_MAX; break;
    case 1: out->b = (uint64_t)SIZE_MAX - (out->a & 0xffu); break;
    case 2: out->a = ((uint64_t)SIZE_MAX / 2u) + (out->a & 0xffffu); break;
    default: break;
    }
    *instance = out;
    return THEFT_ALLOC_OK;
}

static void
prop_two_sizes_free(void *instance, void *env)
{
    (void)env;
    free(instance);
}

static void
prop_two_sizes_print(FILE *f, const void *instance, void *env)
{
    const struct prop_two_sizes *in = (const struct prop_two_sizes *)instance;

    (void)env;
    fprintf(f, "a=%" PRIu64 " b=%" PRIu64 "\n", in->a, in->b);
}

static struct theft_type_info prop_two_sizes_info = {
    .alloc = prop_two_sizes_alloc,
    .free = prop_two_sizes_free,
    .print = prop_two_sizes_print,
    .autoshrink_config = {
        .enable = true,
    },
};

/*
 * Checked size arithmetic must never wrap silently: ducknng_size_add /
 * ducknng_size_mul report overflow as -1 (leaving *out untouched) exactly when
 * the mathematical result exceeds SIZE_MAX, and ducknng_grow_capacity always
 * returns a capacity >= the requested need without overflowing.
 */
static enum theft_trial_res
prop_size_arith_invariants(struct theft *t, void *arg1)
{
    const struct prop_two_sizes *in = (const struct prop_two_sizes *)arg1;
    size_t a = (size_t)in->a;
    size_t b = (size_t)in->b;
    size_t out;
    const size_t sentinel = (size_t)0x5a5a5a5au;

    (void)t;
    out = sentinel;
    if (b > SIZE_MAX - a) {
        if (ducknng_size_add(a, b, &out) != -1) return THEFT_TRIAL_FAIL;
        if (out != sentinel) return THEFT_TRIAL_FAIL;
    } else {
        if (ducknng_size_add(a, b, &out) != 0) return THEFT_TRIAL_FAIL;
        if (out != a + b) return THEFT_TRIAL_FAIL;
    }

    out = sentinel;
    if (a != 0 && b > SIZE_MAX / a) {
        if (ducknng_size_mul(a, b, &out) != -1) return THEFT_TRIAL_FAIL;
        if (out != sentinel) return THEFT_TRIAL_FAIL;
    } else {
        if (ducknng_size_mul(a, b, &out) != 0) return THEFT_TRIAL_FAIL;
        if (out != a * b) return THEFT_TRIAL_FAIL;
    }

    out = 0;
    if (ducknng_grow_capacity(a, b, 256, &out) != 0) return THEFT_TRIAL_FAIL;
    if (out < a) return THEFT_TRIAL_FAIL;

    return THEFT_TRIAL_PASS;
}

static enum theft_trial_res
prop_qk_core_integer_roundtrip(struct theft *t, void *arg1)
{
    const struct prop_two_sizes *in = (const struct prop_two_sizes *)arg1;
    static const uint8_t marker[] = {0x00u, 0x7fu, 0x80u, 0xffu};
    ducknng_qk_writer writer;
    ducknng_qk_reader reader;
    uint8_t encoded[64];
    uint8_t short_buffer[64];
    uint64_t unsigned_value = in->a;
    int64_t signed_value;
    uint64_t decoded_unsigned = 0;
    int64_t decoded_signed = 0;
    const uint8_t *decoded_marker = NULL;
    size_t decoded_marker_size = 0;
    size_t required;
    size_t cut;

    (void)t;
    memcpy(&signed_value, &in->b, sizeof(signed_value));
    ducknng_qk_writer_init_measure(&writer);
    if (ducknng_qk_write_uleb128(&writer, unsigned_value) != 0 ||
        ducknng_qk_write_sleb128(&writer, signed_value) != 0 ||
        ducknng_qk_write_counted(&writer, marker, sizeof(marker)) != 0 ||
        ducknng_qk_writer_status(&writer) != DUCKNNG_QK_OK) {
        return THEFT_TRIAL_FAIL;
    }
    required = ducknng_qk_writer_size(&writer);
    if (required == 0 || required > sizeof(encoded)) return THEFT_TRIAL_FAIL;

    memset(short_buffer, 0xa5, sizeof(short_buffer));
    ducknng_qk_writer_init_fixed(&writer, short_buffer, required - 1u);
    (void)ducknng_qk_write_uleb128(&writer, unsigned_value);
    (void)ducknng_qk_write_sleb128(&writer, signed_value);
    (void)ducknng_qk_write_counted(&writer, marker, sizeof(marker));
    if (ducknng_qk_writer_status(&writer) != DUCKNNG_QK_NO_SPACE ||
        ducknng_qk_writer_size(&writer) > required - 1u ||
        short_buffer[required - 1u] != 0xa5u) return THEFT_TRIAL_FAIL;

    ducknng_qk_writer_init_fixed(&writer, encoded, required);
    if (ducknng_qk_write_uleb128(&writer, unsigned_value) != 0 ||
        ducknng_qk_write_sleb128(&writer, signed_value) != 0 ||
        ducknng_qk_write_counted(&writer, marker, sizeof(marker)) != 0 ||
        ducknng_qk_writer_size(&writer) != required ||
        ducknng_qk_writer_status(&writer) != DUCKNNG_QK_OK) {
        return THEFT_TRIAL_FAIL;
    }

    for (cut = 0; cut < required; cut++) {
        ducknng_qk_reader_init(&reader, encoded, cut);
        if (ducknng_qk_read_uleb128(&reader, &decoded_unsigned) == 0 &&
            ducknng_qk_read_sleb128(&reader, &decoded_signed) == 0 &&
            ducknng_qk_read_counted(&reader, &decoded_marker,
                &decoded_marker_size) == 0) return THEFT_TRIAL_FAIL;
    }
    ducknng_qk_reader_init(&reader, encoded, required);
    if (ducknng_qk_read_uleb128(&reader, &decoded_unsigned) != 0 ||
        ducknng_qk_read_sleb128(&reader, &decoded_signed) != 0 ||
        ducknng_qk_read_counted(&reader, &decoded_marker,
            &decoded_marker_size) != 0 ||
        decoded_unsigned != unsigned_value || decoded_signed != signed_value ||
        decoded_marker_size != sizeof(marker) ||
        memcmp(decoded_marker, marker, sizeof(marker)) != 0 ||
        ducknng_qk_reader_remaining(&reader) != 0 ||
        ducknng_qk_reader_status(&reader) != DUCKNNG_QK_OK) {
        return THEFT_TRIAL_FAIL;
    }
    return THEFT_TRIAL_PASS;
}

#define DUCKNNG_PROP_MAX_PATH_SEG 48u

struct prop_two_strings {
    char a[DUCKNNG_PROP_MAX_PATH_SEG + 1];
    char b[DUCKNNG_PROP_MAX_PATH_SEG + 1];
};

static enum theft_alloc_res
prop_two_strings_alloc(struct theft *t, void *env, void **instance)
{
    struct prop_two_strings *out;
    size_t la, lb, i;

    (void)env;
    out = (struct prop_two_strings *)malloc(sizeof(*out));
    if (!out) return THEFT_ALLOC_ERROR;
    /* Lengths can be 0 so the empty-prefix / empty-name branches are exercised. */
    la = (size_t)prop_random_bounded(t, DUCKNNG_PROP_MAX_PATH_SEG + 1u);
    lb = (size_t)prop_random_bounded(t, DUCKNNG_PROP_MAX_PATH_SEG + 1u);
    /* Fill with printable, never-NUL bytes (incl. '.') so these stay C strings. */
    for (i = 0; i < la; i++) out->a[i] = (char)(0x21u + (theft_random_bits(t, 8) % 0x5eu));
    out->a[la] = '\0';
    for (i = 0; i < lb; i++) out->b[i] = (char)(0x21u + (theft_random_bits(t, 8) % 0x5eu));
    out->b[lb] = '\0';
    *instance = out;
    return THEFT_ALLOC_OK;
}

static void
prop_two_strings_free(void *instance, void *env)
{
    (void)env;
    free(instance);
}

static void
prop_two_strings_print(FILE *f, const void *instance, void *env)
{
    const struct prop_two_strings *in = (const struct prop_two_strings *)instance;

    (void)env;
    fprintf(f, "a=\"%s\" b=\"%s\"\n", in->a, in->b);
}

static struct theft_type_info prop_two_strings_info = {
    .alloc = prop_two_strings_alloc,
    .free = prop_two_strings_free,
    .print = prop_two_strings_print,
    .autoshrink_config = {
        .enable = true,
    },
};

/*
 * ducknng_join_dotted_path(prefix, name) yields a copy of name when prefix is
 * empty, and exactly "prefix.name" (prefix, one '.', name) otherwise — never
 * truncating, overrunning, or losing the NUL terminator.
 */
static enum theft_trial_res
prop_join_dotted_path_invariants(struct theft *t, void *arg1)
{
    const struct prop_two_strings *in = (const struct prop_two_strings *)arg1;
    size_t pa = strlen(in->a);
    size_t pb = strlen(in->b);
    char *r;
    enum theft_trial_res res = THEFT_TRIAL_PASS;

    (void)t;
    r = ducknng_join_dotted_path(in->a, in->b);
    if (!r) return THEFT_TRIAL_FAIL; /* sizes are tiny; allocation must succeed */
    if (pa == 0) {
        if (strcmp(r, in->b) != 0) res = THEFT_TRIAL_FAIL;
    } else if (strlen(r) != pa + 1 + pb) {
        res = THEFT_TRIAL_FAIL;
    } else if (memcmp(r, in->a, pa) != 0 || r[pa] != '.' ||
               (pb && memcmp(r + pa + 1, in->b, pb) != 0)) {
        res = THEFT_TRIAL_FAIL;
    }
    free(r);
    return res;
}

static enum theft_run_res
prop_run_one(const char *name, theft_propfun1 *prop, const struct theft_type_info *info)
{
    struct theft_run_config cfg;

    memset(&cfg, 0, sizeof(cfg));
    cfg.name = name;
    cfg.prop1 = prop;
    cfg.type_info[0] = info;
    cfg.trials = prop_env_size("DUCKNNG_PROP_TRIALS", DUCKNNG_PROP_DEFAULT_TRIALS);
    cfg.seed = prop_env_seed();
    if (prop_env_bool("DUCKNNG_PROP_FORK")) {
        cfg.fork.enable = true;
        cfg.fork.timeout = prop_env_size("DUCKNNG_PROP_TIMEOUT_MS", 1000u);
    }
    return theft_run(&cfg);
}

static enum theft_trial_res
prop_wire_random_bytes(struct theft *t, void *arg1)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)arg1;
    ducknng_frame frame;
    int rc;
    size_t min_len;

    (void)t;
    rc = ducknng_decode_frame_bytes(bytes->data, bytes->len, &frame);
    if (rc != 0) return THEFT_TRIAL_PASS;

    if (frame.version != DUCKNNG_WIRE_VERSION || frame.type > DUCKNNG_RPC_EVENT) {
        return THEFT_TRIAL_FAIL;
    }
    if (frame.name_len > DUCKNNG_MAX_METHOD_NAME_LEN) return THEFT_TRIAL_FAIL;
    if (frame.type == DUCKNNG_RPC_ERROR) {
        if (frame.error_len == 0 ||
            (frame.status > DUCKNNG_STATUS_DISABLED &&
             frame.status != DUCKNNG_STATUS_UNSPECIFIED)) return THEFT_TRIAL_FAIL;
    } else if (frame.error_len != 0 || frame.status != DUCKNNG_STATUS_OK) {
        return THEFT_TRIAL_FAIL;
    }
    min_len = DUCKNNG_WIRE_HEADER_LEN + (size_t)frame.name_len + (size_t)frame.error_len;
    if (min_len > bytes->len) return THEFT_TRIAL_FAIL;
    if (frame.payload_len != (uint64_t)(bytes->len - min_len)) return THEFT_TRIAL_FAIL;
    if (frame.name != bytes->data + DUCKNNG_WIRE_HEADER_LEN) return THEFT_TRIAL_FAIL;
    if (frame.error != frame.name + frame.name_len) return THEFT_TRIAL_FAIL;
    if (frame.payload != frame.error + frame.error_len) return THEFT_TRIAL_FAIL;
    return THEFT_TRIAL_PASS;
}

static enum theft_trial_res
prop_wire_valid_frame_decodes(struct theft *t, void *arg1)
{
    const struct prop_frame *input = (const struct prop_frame *)arg1;
    ducknng_frame frame;
    size_t name_off = DUCKNNG_WIRE_HEADER_LEN;
    size_t err_off = name_off + input->name_len;
    size_t payload_off = err_off + input->error_len;
    size_t cut;

    (void)t;
    if (ducknng_decode_frame_bytes(input->data, input->len, &frame) != 0) {
        return THEFT_TRIAL_FAIL;
    }
    if (frame.version != DUCKNNG_WIRE_VERSION || frame.type != input->type ||
        frame.status != input->status || frame.flags != input->flags ||
        frame.name_len != input->name_len ||
        frame.error_len != input->error_len || frame.payload_len != input->payload_len) {
        return THEFT_TRIAL_FAIL;
    }
    if (memcmp(frame.name, input->data + name_off, input->name_len) != 0) {
        return THEFT_TRIAL_FAIL;
    }
    if (memcmp(frame.error, input->data + err_off, input->error_len) != 0) {
        return THEFT_TRIAL_FAIL;
    }
    if (memcmp(frame.payload, input->data + payload_off, (size_t)input->payload_len) != 0) {
        return THEFT_TRIAL_FAIL;
    }
    for (cut = 0; cut < input->len; cut++) {
        if (ducknng_decode_frame_bytes(input->data, cut, &frame) == 0) {
            return THEFT_TRIAL_FAIL;
        }
    }
    {
        uint8_t *with_suffix = (uint8_t *)malloc(input->len + 1u);
        int rc;
        if (!with_suffix) return THEFT_TRIAL_ERROR;
        memcpy(with_suffix, input->data, input->len);
        with_suffix[input->len] = 0xa5u;
        rc = ducknng_decode_frame_bytes(with_suffix, input->len + 1u, &frame);
        free(with_suffix);
        if (rc == 0) return THEFT_TRIAL_FAIL;
    }
    return THEFT_TRIAL_PASS;
}

static enum theft_trial_res
prop_transport_random_urls(struct theft *t, void *arg1)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)arg1;
    char *url;
    char *errmsg = NULL;
    ducknng_transport_url parsed;
    int rc;
    enum theft_trial_res result = THEFT_TRIAL_PASS;

    (void)t;
    url = (char *)malloc(bytes->len + 1);
    if (!url) return THEFT_TRIAL_ERROR;
    if (bytes->len) memcpy(url, bytes->data, bytes->len);
    url[bytes->len] = '\0';
    ducknng_transport_url_init(&parsed);
    rc = ducknng_transport_url_parse(url, &parsed, &errmsg);
    if (rc == 0) {
        if (errmsg != NULL) result = THEFT_TRIAL_FAIL;
        if (parsed.family != DUCKNNG_TRANSPORT_FAMILY_NNG &&
            parsed.family != DUCKNNG_TRANSPORT_FAMILY_HTTP) {
            result = THEFT_TRIAL_FAIL;
        }
        if (parsed.uses_tls && parsed.scheme != DUCKNNG_TRANSPORT_SCHEME_TLS_TCP &&
            parsed.scheme != DUCKNNG_TRANSPORT_SCHEME_WSS &&
            parsed.scheme != DUCKNNG_TRANSPORT_SCHEME_HTTPS) {
            result = THEFT_TRIAL_FAIL;
        }
        if (ducknng_transport_url_is_nng(&parsed) && parsed.family != DUCKNNG_TRANSPORT_FAMILY_NNG) {
            result = THEFT_TRIAL_FAIL;
        }
        if (ducknng_transport_url_is_http(&parsed) && parsed.family != DUCKNNG_TRANSPORT_FAMILY_HTTP) {
            result = THEFT_TRIAL_FAIL;
        }
        if (!ducknng_transport_family_name(parsed.family) || !ducknng_transport_scheme_name(parsed.scheme)) {
            result = THEFT_TRIAL_FAIL;
        }
    }
    if (errmsg) free(errmsg);
    free(url);
    return result;
}

static enum theft_trial_res
prop_quack_random_payloads(struct theft *t, void *arg1)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)arg1;
    ducknng_quack_schema schema;
    idx_t row_count = 0;
    size_t offset = 0;
    uint64_t remaining = 0;
    char *errmsg = NULL;
    int rc;
    enum theft_trial_res result = THEFT_TRIAL_PASS;

    (void)t;
    memset(&schema, 0, sizeof(schema));
    rc = ducknng_quack_payload_read_row_count(bytes->data, bytes->len, &schema,
        &row_count, &errmsg);
    if (rc == 0 && errmsg != NULL) result = THEFT_TRIAL_FAIL;
    if (errmsg) {
        free(errmsg);
        errmsg = NULL;
    }
    rc = ducknng_quack_payload_scan_begin(bytes->data, bytes->len, &schema,
        &offset, &remaining, &errmsg);
    if (rc == 0 && (errmsg != NULL || offset > bytes->len)) result = THEFT_TRIAL_FAIL;
    if (errmsg) free(errmsg);
    return result;
}

/* Wire logical-type ids mirror the TU-private DUCKNNG_QUACK_LOGICAL_* defines in
 * src/ducknng_quack.c; kept local so the property harness can hand-build a nested
 * schema and fuzz the recursive skip path without widening the public header. */
#define PROP_QK_INTEGER 13
#define PROP_QK_BIGINT  14
#define PROP_QK_DECIMAL 21
#define PROP_QK_VARCHAR 25
#define PROP_QK_STRUCT  100
#define PROP_QK_LIST    101
#define PROP_QK_MAP     102
#define PROP_QK_ENUM    104
#define PROP_QK_ARRAY   108

#define PROP_QK_FIELD_END          0xffffu
#define PROP_QK_OUTER_RESULT_TYPES 1u
#define PROP_QK_OUTER_RESULT_NAMES 2u
#define PROP_QK_OUTER_RESULTS      4u
#define PROP_QK_TYPE_ID            100u
#define PROP_QK_TYPE_INFO          101u
#define PROP_QK_EXTRA_INFO_KIND    100u
#define PROP_QK_EXTRA_CHILD        200u
#define PROP_QK_EXTRA_ARRAY_SIZE   201u
#define PROP_QK_EXTRA_TYPE_DECIMAL   2u
#define PROP_QK_EXTRA_TYPE_LIST      4u
#define PROP_QK_EXTRA_TYPE_STRUCT    5u
#define PROP_QK_EXTRA_TYPE_ENUM      6u
#define PROP_QK_EXTRA_TYPE_ARRAY     9u
#define PROP_QK_CHUNK_WRAPPER      300u
#define PROP_QK_CHUNK_ROWS          100u
#define PROP_QK_CHUNK_COLUMNS       102u
#define PROP_QK_VECTOR_TYPE          90u
#define PROP_QK_VECTOR_SELECTION     91u
#define PROP_QK_VECTOR_AUX           92u
#define PROP_QK_VECTOR_HAS_VALIDITY 100u
#define PROP_QK_VECTOR_DATA         102u
#define PROP_QK_VECTOR_CONSTANT       2u
#define PROP_QK_VECTOR_DICTIONARY     3u
#define PROP_QK_VECTOR_SEQUENCE       4u

struct prop_quack_buf {
    uint8_t data[256];
    size_t len;
};

static int
prop_qb_put(struct prop_quack_buf *b, const void *src, size_t len)
{
    if (!b || len > sizeof(b->data) || b->len > sizeof(b->data) - len) return -1;
    if (len) memcpy(b->data + b->len, src, len);
    b->len += len;
    return 0;
}

static int
prop_qb_byte(struct prop_quack_buf *b, uint8_t value)
{
    return prop_qb_put(b, &value, 1);
}

static int
prop_qb_u16(struct prop_quack_buf *b, uint16_t value)
{
    uint8_t tmp[2];

    tmp[0] = (uint8_t)(value & 0xffu);
    tmp[1] = (uint8_t)((value >> 8) & 0xffu);
    return prop_qb_put(b, tmp, sizeof(tmp));
}

static int
prop_qb_uleb(struct prop_quack_buf *b, uint64_t value)
{
    do {
        uint8_t byte = (uint8_t)(value & 0x7fu);
        value >>= 7;
        if (value) byte |= 0x80u;
        if (prop_qb_byte(b, byte) != 0) return -1;
    } while (value);
    return 0;
}

static int
prop_qb_sleb(struct prop_quack_buf *b, int64_t value)
{
    int more = 1;

    while (more) {
        int64_t next = value / 128;
        uint8_t byte;
        int sign;

        if (value < 0 && value % 128 != 0) next--;
        byte = (uint8_t)(value - next * 128);
        sign = (byte & 0x40u) != 0;
        more = !((next == 0 && !sign) || (next == -1 && sign));
        if (more) byte |= 0x80u;
        if (prop_qb_byte(b, byte) != 0) return -1;
        value = next;
    }
    return 0;
}

static int
prop_qb_blob(struct prop_quack_buf *b, const void *src, size_t len)
{
    return prop_qb_uleb(b, (uint64_t)len) != 0 ||
        prop_qb_put(b, src, len) != 0 ? -1 : 0;
}

static int
prop_qb_field_end(struct prop_quack_buf *b)
{
    return prop_qb_u16(b, PROP_QK_FIELD_END);
}

static int
prop_quack_build_one_col_schema(ducknng_quack_schema *schema, int type_id)
{
    memset(schema, 0, sizeof(*schema));
    schema->cols = (ducknng_quack_column_schema *)malloc(sizeof(*schema->cols));
    if (!schema->cols) return -1;
    memset(schema->cols, 0, sizeof(*schema->cols));
    schema->ncols = 1;
    schema->cols[0].logical_type_id = type_id;
    return 0;
}

static int
prop_qb_begin_one_col_chunk(struct prop_quack_buf *b, uint64_t rows)
{
    return prop_qb_u16(b, PROP_QK_OUTER_RESULTS) != 0 ||
        prop_qb_uleb(b, 1) != 0 ||
        prop_qb_byte(b, 1) != 0 ||
        prop_qb_u16(b, PROP_QK_CHUNK_WRAPPER) != 0 ||
        prop_qb_u16(b, PROP_QK_CHUNK_ROWS) != 0 ||
        prop_qb_uleb(b, rows) != 0 ||
        prop_qb_u16(b, PROP_QK_CHUNK_COLUMNS) != 0 ||
        prop_qb_uleb(b, 1) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_HAS_VALIDITY) != 0 ||
        prop_qb_byte(b, 0) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_DATA) != 0 ? -1 : 0;
}

static int
prop_qb_begin_one_col_result_vector(struct prop_quack_buf *b, uint64_t rows)
{
    memset(b, 0, sizeof(*b));
    return prop_qb_u16(b, PROP_QK_OUTER_RESULTS) != 0 ||
        prop_qb_uleb(b, 1) != 0 ||
        prop_qb_byte(b, 1) != 0 ||
        prop_qb_u16(b, PROP_QK_CHUNK_WRAPPER) != 0 ||
        prop_qb_u16(b, PROP_QK_CHUNK_ROWS) != 0 ||
        prop_qb_uleb(b, rows) != 0 ||
        prop_qb_u16(b, PROP_QK_CHUNK_COLUMNS) != 0 ||
        prop_qb_uleb(b, 1) != 0 ? -1 : 0;
}

static int
prop_qb_finish_one_col_result_vector(struct prop_quack_buf *b)
{
    /* vector, DataChunk, DataChunkWrapper, top-level object */
    return prop_qb_field_end(b) != 0 || prop_qb_field_end(b) != 0 ||
        prop_qb_field_end(b) != 0 || prop_qb_field_end(b) != 0 ? -1 : 0;
}

static int
prop_quack_payload_constant(struct prop_quack_buf *b)
{
    int64_t value = 42;

    if (prop_qb_begin_one_col_result_vector(b, 4) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_TYPE) != 0 ||
        prop_qb_uleb(b, PROP_QK_VECTOR_CONSTANT) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_HAS_VALIDITY) != 0 ||
        prop_qb_byte(b, 0) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_DATA) != 0 ||
        prop_qb_blob(b, &value, sizeof(value)) != 0) return -1;
    return prop_qb_finish_one_col_result_vector(b);
}

static int
prop_quack_payload_dictionary(struct prop_quack_buf *b, int malformed_selection)
{
    uint32_t selection[5] = {2, 0, 2, 1, 0};
    static const char *values[3] = {"a", "b", "c"};
    size_t i;

    if (malformed_selection) selection[0] = 3;
    if (prop_qb_begin_one_col_result_vector(b, 5) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_TYPE) != 0 ||
        prop_qb_uleb(b, PROP_QK_VECTOR_DICTIONARY) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_SELECTION) != 0 ||
        prop_qb_blob(b, selection, sizeof(selection)) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_AUX) != 0 ||
        prop_qb_uleb(b, 3) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_HAS_VALIDITY) != 0 ||
        prop_qb_byte(b, 0) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_DATA) != 0 ||
        prop_qb_uleb(b, 3) != 0) return -1;
    for (i = 0; i < 3; i++) {
        if (prop_qb_blob(b, values[i], strlen(values[i])) != 0) return -1;
    }
    return prop_qb_finish_one_col_result_vector(b);
}

static int
prop_quack_payload_sequence(struct prop_quack_buf *b)
{
    if (prop_qb_begin_one_col_result_vector(b, 4) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_TYPE) != 0 ||
        prop_qb_uleb(b, PROP_QK_VECTOR_SEQUENCE) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_SELECTION) != 0 ||
        prop_qb_sleb(b, 10) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_AUX) != 0 ||
        prop_qb_sleb(b, -2) != 0) return -1;
    return prop_qb_finish_one_col_result_vector(b);
}

static int
prop_quack_payload_invalid_validity_boolean(struct prop_quack_buf *b)
{
    int64_t value = 42;
    if (prop_qb_begin_one_col_result_vector(b, 1) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_HAS_VALIDITY) != 0 ||
        prop_qb_byte(b, 2) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_DATA) != 0 ||
        prop_qb_blob(b, &value, sizeof(value)) != 0) return -1;
    return prop_qb_finish_one_col_result_vector(b);
}

static int
prop_quack_payload_invalid_chunk_boolean(struct prop_quack_buf *b)
{
    memset(b, 0, sizeof(*b));
    return prop_qb_u16(b, PROP_QK_OUTER_RESULTS) != 0 ||
        prop_qb_uleb(b, 1) != 0 ||
        prop_qb_byte(b, 2) != 0 ||
        prop_qb_field_end(b) != 0 ? -1 : 0;
}

static int
prop_quack_payload_fixed_width_overflow(struct prop_quack_buf *b)
{
    memset(b, 0, sizeof(*b));
    if (prop_qb_begin_one_col_chunk(b, UINT64_C(1) << 61) != 0) return -1;
    if (prop_qb_uleb(b, 0) != 0) return -1;
    return prop_qb_field_end(b) != 0 || prop_qb_field_end(b) != 0 ||
        prop_qb_field_end(b) != 0 ? -1 : 0;
}

static int
prop_quack_payload_huge_schema_column_count(struct prop_quack_buf *b)
{
    uint64_t ncols;

    memset(b, 0, sizeof(*b));
    ncols = UINT64_MAX / (uint64_t)sizeof(ducknng_quack_column_schema) + 2u;
    return prop_qb_u16(b, PROP_QK_OUTER_RESULT_TYPES) != 0 ||
        prop_qb_uleb(b, ncols) != 0 ? -1 : 0;
}

static int
prop_quack_payload_varlen_wraparound(struct prop_quack_buf *b)
{
    size_t first_data_off;
    size_t after_huge_len_off;
    uint64_t huge_len;
    uint8_t fake_ends[6] = {0xffu, 0xffu, 0xffu, 0xffu, 0xffu, 0xffu};

    memset(b, 0, sizeof(*b));
    if (prop_qb_begin_one_col_chunk(b, 2) != 0) return -1;
    if (prop_qb_uleb(b, 2) != 0) return -1;
    if (prop_qb_uleb(b, sizeof(fake_ends)) != 0) return -1;
    first_data_off = b->len;
    if (prop_qb_put(b, fake_ends, sizeof(fake_ends)) != 0) return -1;

    /* Choose a second string length that made the old size_t bounds check wrap
     * r->off back to first_data_off, where fake field-end bytes are waiting. */
    after_huge_len_off = b->len + 10;
    huge_len = UINT64_MAX - (uint64_t)after_huge_len_off + 1u +
        (uint64_t)first_data_off;
    if (prop_qb_uleb(b, huge_len) != 0) return -1;
    return b->len == after_huge_len_off ? 0 : -1;
}

static ducknng_quack_column_schema *prop_quack_leaf(int type_id)
{
    ducknng_quack_column_schema *n =
        (ducknng_quack_column_schema *)malloc(sizeof(*n));
    if (!n) return NULL;
    memset(n, 0, sizeof(*n));
    n->logical_type_id = type_id;
    return n;
}

/* Build STRUCT(a INTEGER, b VARCHAR) and LIST(INTEGER) top-level columns. The
 * tree is owned through ducknng_quack_schema_reset, matching the codec's own
 * allocation/ownership rules (duckdb_malloc == malloc in this harness). */
static int prop_quack_build_nested_schema(ducknng_quack_schema *schema)
{
    ducknng_quack_column_schema *st;
    ducknng_quack_column_schema *li;
    memset(schema, 0, sizeof(*schema));
    schema->cols = (ducknng_quack_column_schema *)malloc(sizeof(*schema->cols) * 2);
    if (!schema->cols) return -1;
    memset(schema->cols, 0, sizeof(*schema->cols) * 2);
    schema->ncols = 2;
    st = &schema->cols[0];
    st->logical_type_id = PROP_QK_STRUCT;
    st->nchildren = 2;
    st->children = (ducknng_quack_column_schema **)malloc(sizeof(*st->children) * 2);
    st->child_names = (char **)malloc(sizeof(*st->child_names) * 2);
    if (!st->children || !st->child_names) return -1;
    st->children[0] = prop_quack_leaf(PROP_QK_INTEGER);
    st->children[1] = prop_quack_leaf(PROP_QK_VARCHAR);
    st->child_names[0] = NULL;
    st->child_names[1] = NULL;
    if (!st->children[0] || !st->children[1]) return -1;
    li = &schema->cols[1];
    li->logical_type_id = PROP_QK_LIST;
    li->nchildren = 1;
    li->children = (ducknng_quack_column_schema **)malloc(sizeof(*li->children) * 1);
    if (!li->children) return -1;
    li->children[0] = prop_quack_leaf(PROP_QK_INTEGER);
    if (!li->children[0]) return -1;
    return 0;
}

/* Fuzz the recursive nested skip path: a fixed STRUCT+LIST schema parsed against
 * random bytes must reject cleanly (or parse a row count) without crashing and
 * without claiming success while leaving an error string. */
static enum theft_trial_res
prop_quack_nested_random_payloads(struct theft *t, void *arg1)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)arg1;
    ducknng_quack_schema schema;
    idx_t row_count = 0;
    char *errmsg = NULL;
    int rc;
    enum theft_trial_res result = THEFT_TRIAL_PASS;

    (void)t;
    if (prop_quack_build_nested_schema(&schema) != 0) {
        ducknng_quack_schema_reset(&schema);
        return THEFT_TRIAL_SKIP;
    }
    rc = ducknng_quack_payload_read_row_count(bytes->data, bytes->len, &schema,
        &row_count, &errmsg);
    if (rc == 0 && errmsg != NULL) result = THEFT_TRIAL_FAIL;
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    return result;
}

/* Build a minimal parse_schema-acceptable payload: one INTEGER column whose
 * name is the given counted bytes. Layout mirrors ducknng_quack_reader_parse_schema:
 *   [u16 RESULT_TYPES][uleb 1][type node: u16 TYPE_ID, uleb INTEGER, u16 END]
 *   [u16 RESULT_NAMES][uleb 1][uleb name_len][name bytes]
 * Used to prove the embedded-NUL guard rejects a hostile column name while an
 * otherwise-identical NUL-free name still parses. */
static int
prop_quack_build_one_col_named_schema(struct prop_quack_buf *b,
    const uint8_t *name, size_t name_len)
{
    memset(b, 0, sizeof(*b));
    if (prop_qb_u16(b, PROP_QK_OUTER_RESULT_TYPES) != 0) return -1;
    if (prop_qb_uleb(b, 1) != 0) return -1;
    if (prop_qb_u16(b, PROP_QK_TYPE_ID) != 0) return -1;
    if (prop_qb_uleb(b, PROP_QK_INTEGER) != 0) return -1;
    if (prop_qb_field_end(b) != 0) return -1;
    if (prop_qb_u16(b, PROP_QK_OUTER_RESULT_NAMES) != 0) return -1;
    if (prop_qb_uleb(b, 1) != 0) return -1;
    if (prop_qb_uleb(b, (uint64_t)name_len) != 0) return -1;
    return prop_qb_put(b, name, name_len);
}

static int
prop_quack_schema_begin(struct prop_quack_buf *b, uint64_t logical_type,
    uint8_t info_present)
{
    memset(b, 0, sizeof(*b));
    return prop_qb_u16(b, PROP_QK_OUTER_RESULT_TYPES) != 0 ||
        prop_qb_uleb(b, 1) != 0 ||
        prop_qb_u16(b, PROP_QK_TYPE_ID) != 0 ||
        prop_qb_uleb(b, logical_type) != 0 ||
        prop_qb_u16(b, PROP_QK_TYPE_INFO) != 0 ||
        prop_qb_byte(b, info_present) != 0 ? -1 : 0;
}

static int
prop_quack_schema_finish(struct prop_quack_buf *b)
{
    static const uint8_t name[] = {'x'};
    return prop_qb_field_end(b) != 0 ||
        prop_qb_u16(b, PROP_QK_OUTER_RESULT_NAMES) != 0 ||
        prop_qb_uleb(b, 1) != 0 ||
        prop_qb_blob(b, name, sizeof(name)) != 0 ? -1 : 0;
}

static int
prop_quack_put_primitive_type(struct prop_quack_buf *b, uint64_t logical_type)
{
    return prop_qb_u16(b, PROP_QK_TYPE_ID) != 0 ||
        prop_qb_uleb(b, logical_type) != 0 ||
        prop_qb_field_end(b) != 0 ? -1 : 0;
}

static int
prop_quack_schema_rejects(const struct prop_quack_buf *b)
{
    ducknng_quack_schema schema;
    char *errmsg = NULL;
    int rc;
    memset(&schema, 0, sizeof(schema));
    rc = ducknng_quack_payload_parse_schema(b->data, b->len, &schema, &errmsg);
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    return rc == 0 ? -1 : 0;
}

/* Reject and prove which check fired, so a regression that starts rejecting a
 * payload for an unrelated reason still fails the test. */
static int
prop_quack_schema_rejects_with(const struct prop_quack_buf *b,
    const char *expect_substring)
{
    ducknng_quack_schema schema;
    char *errmsg = NULL;
    int rc;
    int matched;
    memset(&schema, 0, sizeof(schema));
    rc = ducknng_quack_payload_parse_schema(b->data, b->len, &schema, &errmsg);
    matched = rc != 0 && errmsg != NULL &&
        strstr(errmsg, expect_substring) != NULL;
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    return matched ? 0 : -1;
}

static int
prop_quack_schema_accepts(const struct prop_quack_buf *b)
{
    ducknng_quack_schema schema;
    char *errmsg = NULL;
    int rc;
    memset(&schema, 0, sizeof(schema));
    rc = ducknng_quack_payload_parse_schema(b->data, b->len, &schema, &errmsg);
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    return rc == 0 ? 0 : -1;
}

/* The server upload append path parses an attacker-supplied quack header via
 * ducknng_quack_payload_parse_schema. Random bytes must reject cleanly or
 * parse a bounded schema, never crash, and never return success with an error
 * string set. On success the schema must be internally consistent (bounded
 * column count, cols present iff ncols>0) and free cleanly. */
static enum theft_trial_res
prop_quack_parse_schema_random_payloads(struct theft *t, void *arg1)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)arg1;
    ducknng_quack_schema schema;
    char *errmsg = NULL;
    int rc;
    enum theft_trial_res result = THEFT_TRIAL_PASS;

    (void)t;
    memset(&schema, 0, sizeof(schema));
    rc = ducknng_quack_payload_parse_schema(bytes->data, bytes->len, &schema, &errmsg);
    if (rc == 0 && errmsg != NULL) result = THEFT_TRIAL_FAIL;
    if (rc == 0) {
        if (schema.ncols > 0 && schema.cols == NULL) result = THEFT_TRIAL_FAIL;
        if (schema.ncols == 0 && schema.cols != NULL) result = THEFT_TRIAL_FAIL;
    }
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    return result;
}

/* The upload-append frame prefix is parsed from attacker-supplied bytes on the
 * server. Random bytes must never crash, and on success the reported token
 * slice and quack offset must lie within the buffer, be internally
 * consistent, and round-trip against the fixed layout. */
static enum theft_trial_res
prop_upload_prefix_random_bytes(struct theft *t, void *arg1)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)arg1;
    uint64_t session_id = 0;
    const uint8_t *token = NULL;
    size_t token_len = 0;
    size_t quack_off = 0;
    int rc;

    (void)t;
    rc = ducknng_upload_append_parse_prefix(bytes->data, bytes->len,
        &session_id, &token, &token_len, &quack_off);
    if (rc != 0) return THEFT_TRIAL_PASS; /* rejected cleanly */
    /* On success every reported slice must be inside the buffer. */
    if (token_len == 0 || token_len > DUCKNNG_UPLOAD_TOKEN_MAX) return THEFT_TRIAL_FAIL;
    if (!token) return THEFT_TRIAL_FAIL;
    if (token != bytes->data + 10) return THEFT_TRIAL_FAIL;
    if ((size_t)(token - bytes->data) + token_len != quack_off) return THEFT_TRIAL_FAIL;
    if (quack_off > bytes->len) return THEFT_TRIAL_FAIL;
    return THEFT_TRIAL_PASS;
}

TEST wire_rejects_or_decodes_random_bytes(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("wire random bytes", prop_wire_random_bytes, &prop_random_bytes_info));
    PASS();
}

TEST upload_prefix_parses_valid_and_rejects_short(void)
{
    /* Valid: session_id=0x0102030405060708, token_len=3 "abc", then 2 quack bytes. */
    uint8_t buf[15] = {8,7,6,5,4,3,2,1, 3,0, 'a','b','c', 0xAA,0xBB};
    uint64_t sid = 0;
    const uint8_t *tok = NULL;
    size_t tok_len = 0, quack_off = 0;
    ASSERT_EQ(0, ducknng_upload_append_parse_prefix(buf, sizeof(buf), &sid, &tok, &tok_len, &quack_off));
    ASSERT_EQ((uint64_t)0x0102030405060708ULL, sid);
    ASSERT_EQ((size_t)3, tok_len);
    ASSERT_EQ(buf + 10, tok);
    ASSERT_EQ((size_t)13, quack_off);
    /* An empty quack remainder is allowed. */
    ASSERT_EQ(0, ducknng_upload_append_parse_prefix(buf, 13, &sid, &tok, &tok_len, &quack_off));
    ASSERT_EQ((size_t)13, quack_off);
    /* Too short for the fixed header, zero token length, and token overrunning
     * the buffer all reject. */
    ASSERT_EQ(-1, ducknng_upload_append_parse_prefix(buf, 9, &sid, &tok, &tok_len, &quack_off));
    { uint8_t z[12] = {0,0,0,0,0,0,0,0, 0,0, 1,2};
      ASSERT_EQ(-1, ducknng_upload_append_parse_prefix(z, sizeof(z), &sid, &tok, &tok_len, &quack_off)); }
    { uint8_t over[12] = {0,0,0,0,0,0,0,0, 5,0, 1,2};
      ASSERT_EQ(-1, ducknng_upload_append_parse_prefix(over, sizeof(over), &sid, &tok, &tok_len, &quack_off)); }
    PASS();
}

TEST upload_prefix_rejects_random_bytes(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("upload append prefix random bytes", prop_upload_prefix_random_bytes,
            &prop_random_bytes_info));
    PASS();
}

TEST wire_decodes_generated_valid_frames(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("wire valid frames", prop_wire_valid_frame_decodes, &prop_frame_info));
    PASS();
}

TEST wire_status_roundtrips_and_invalid_assignments_reject(void)
{
    static const uint8_t method[] = "method";
    static const uint8_t message[] = "failure";
    static const uint8_t payload[] = {0x00u, 0x7fu, 0xffu};
    ducknng_frame_parts parts;
    ducknng_frame frame;
    uint8_t bytes[128];
    size_t required = 0;
    size_t written = 0;
    int status;

    memset(&parts, 0, sizeof(parts));
    parts.type = DUCKNNG_RPC_ERROR;
    parts.name = method;
    parts.name_len = sizeof(method) - 1u;
    parts.error = message;
    parts.error_len = sizeof(message) - 1u;
    parts.payload = payload;
    parts.payload_len = sizeof(payload);
    for (status = DUCKNNG_STATUS_INVALID;
         status <= DUCKNNG_STATUS_DISABLED; status++) {
        parts.status = status;
        ASSERT_EQ(0, ducknng_frame_measure(&parts, &required));
        ASSERT(required <= sizeof(bytes));
        written = 0;
        ASSERT_EQ(0, ducknng_encode_frame_bytes(&parts, bytes,
            sizeof(bytes), &written));
        ASSERT_EQ(required, written);
        ASSERT_EQ(0, ducknng_decode_frame_bytes(bytes, written, &frame));
        ASSERT_EQ(status, frame.status);
        ASSERT_EQ((uint32_t)0, frame.flags);
        ASSERT_EQ(sizeof(payload), (size_t)frame.payload_len);
        ASSERT_MEM_EQ(payload, frame.payload, sizeof(payload));
    }

    /* A zero high status byte is accepted only as a legacy error frame and is
     * represented explicitly rather than confused with success. */
    parts.status = DUCKNNG_STATUS_INVALID;
    ASSERT_EQ(0, ducknng_encode_frame_bytes(&parts, bytes,
        sizeof(bytes), &written));
    bytes[5] = 0;
    ASSERT_EQ(0, ducknng_decode_frame_bytes(bytes, written, &frame));
    ASSERT_EQ(DUCKNNG_STATUS_UNSPECIFIED, frame.status);

    parts.status = DUCKNNG_STATUS_UNSPECIFIED;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.status = DUCKNNG_STATUS_DISABLED + 1;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.type = DUCKNNG_RPC_RESULT;
    parts.status = DUCKNNG_STATUS_INVALID;
    parts.error = NULL;
    parts.error_len = 0;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));

    /* The decoder rejects status bits on successful frames, unknown types,
     * missing error text, and bytes after the counted payload. */
    parts.status = DUCKNNG_STATUS_OK;
    ASSERT_EQ(0, ducknng_encode_frame_bytes(&parts, bytes,
        sizeof(bytes), &written));
    bytes[5] = DUCKNNG_STATUS_INVALID;
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes, written, &frame));
    bytes[5] = 0;
    bytes[1] = 0xffu;
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes, written, &frame));
    bytes[1] = DUCKNNG_RPC_ERROR;
    prop_write_le32(bytes + 10, 0);
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes, written, &frame));
    PASS();
}

TEST wire_core_exercises_explicit_api_edges(void)
{
    static const uint8_t name[] = {'n'};
    static const uint8_t error[] = {'e'};
    static const uint8_t payload[] = {'p'};
    ducknng_frame_parts parts;
    ducknng_frame frame;
    uint8_t bytes[64];
    uint8_t prefix[16] = {0};
    uint64_t session_id = 0;
    const uint8_t *token = NULL;
    size_t token_len = 0;
    size_t quack_offset = 0;
    size_t required = 0;
    size_t written = 0;

    ASSERT_EQ(-1, ducknng_frame_measure(NULL, &required));
    memset(&parts, 0, sizeof(parts));
    parts.type = DUCKNNG_RPC_RESULT;
    parts.name = name;
    parts.name_len = sizeof(name);
    parts.payload = payload;
    parts.payload_len = sizeof(payload);
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, NULL));

    parts.type = 0xffu;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.type = DUCKNNG_RPC_RESULT;
    parts.name_len = DUCKNNG_MAX_METHOD_NAME_LEN + 1u;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.name_len = sizeof(name);
#if SIZE_MAX > UINT32_MAX
    parts.type = DUCKNNG_RPC_ERROR;
    parts.status = DUCKNNG_STATUS_INVALID;
    parts.error = error;
    parts.error_len = (size_t)UINT32_MAX + 1u;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.type = DUCKNNG_RPC_RESULT;
    parts.status = DUCKNNG_STATUS_OK;
    parts.error = NULL;
    parts.error_len = 0;
#endif
    parts.flags = DUCKNNG_RPC_STATUS_MASK;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.flags = 0;
    parts.name = NULL;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.name = name;
    parts.payload = NULL;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.payload = payload;

    parts.type = DUCKNNG_RPC_ERROR;
    parts.status = DUCKNNG_STATUS_INVALID;
    parts.error = NULL;
    parts.error_len = sizeof(error);
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.error = error;
    parts.error_len = 0;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.error_len = sizeof(error);
    parts.status = DUCKNNG_STATUS_OK;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.status = DUCKNNG_STATUS_DISABLED + 1;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));

    parts.type = DUCKNNG_RPC_RESULT;
    parts.status = DUCKNNG_STATUS_OK;
    parts.error = error;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.error = NULL;
    parts.error_len = 0;
    parts.payload_len = SIZE_MAX;
    ASSERT_EQ(-1, ducknng_frame_measure(&parts, &required));
    parts.payload_len = sizeof(payload);
    ASSERT_EQ(0, ducknng_frame_measure(&parts, &required));
    parts.payload = NULL;
    ASSERT_EQ(-1, ducknng_encode_frame_prefix(&parts, bytes,
        sizeof(bytes), NULL));
    written = 0;
    ASSERT_EQ(-1, ducknng_encode_frame_prefix(&parts, NULL, 0, &written));
    ASSERT_EQ(DUCKNNG_WIRE_HEADER_LEN + sizeof(name), written);
    ASSERT_EQ(-1, ducknng_encode_frame_prefix(&parts, bytes,
        written - 1u, &written));
    ASSERT_EQ(0, ducknng_encode_frame_prefix(&parts, bytes,
        sizeof(bytes), &written));
    ASSERT_EQ(DUCKNNG_WIRE_HEADER_LEN + sizeof(name), written);
    parts.payload = payload;
    parts.type = 0xffu;
    ASSERT_EQ(-1, ducknng_encode_frame_bytes(&parts, bytes,
        sizeof(bytes), &written));
    parts.type = DUCKNNG_RPC_RESULT;
    {
        ducknng_frame_parts empty_parts;
        memset(&empty_parts, 0, sizeof(empty_parts));
        empty_parts.type = DUCKNNG_RPC_RESULT;
        ASSERT_EQ(0, ducknng_frame_measure(&empty_parts, &required));
    }
    ASSERT_EQ(0, ducknng_frame_measure(&parts, &required));
    ASSERT_EQ(-1, ducknng_encode_frame_bytes(&parts, bytes,
        sizeof(bytes), NULL));
    written = 0;
    ASSERT_EQ(-1, ducknng_encode_frame_bytes(&parts, NULL, 0, &written));
    ASSERT_EQ(required, written);
    written = 0;
    ASSERT_EQ(-1, ducknng_encode_frame_bytes(&parts, bytes,
        required - 1u, &written));
    ASSERT_EQ(required, written);
    ASSERT_EQ(0, ducknng_encode_frame_bytes(&parts, bytes,
        sizeof(bytes), &written));

    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes, written, NULL));
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(NULL, written, &frame));
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes,
        DUCKNNG_WIRE_HEADER_LEN - 1u, &frame));
    bytes[0] = 0;
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes, written, &frame));
    bytes[0] = DUCKNNG_WIRE_VERSION;
    bytes[1] = 0xffu;
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes, written, &frame));
    bytes[1] = DUCKNNG_RPC_RESULT;
    prop_write_le32(bytes + 6, DUCKNNG_MAX_METHOD_NAME_LEN + 1u);
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes, written, &frame));
    prop_write_le32(bytes + 6, sizeof(name));
    prop_write_le32(bytes + 10, UINT32_MAX);
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes, written, &frame));
    prop_write_le32(bytes + 10, 0);
    prop_write_le64(bytes + 14, 2);
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes, written, &frame));
    prop_write_le64(bytes + 14, sizeof(payload));
    ASSERT_EQ(0, ducknng_decode_frame_bytes(bytes, written, &frame));
    bytes[1] = DUCKNNG_RPC_ERROR;
    prop_write_le32(bytes + 2,
        (uint32_t)(DUCKNNG_STATUS_DISABLED + 1) << DUCKNNG_RPC_STATUS_SHIFT);
    prop_write_le32(bytes + 10, 1);
    prop_write_le64(bytes + 14, 0);
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes, written, &frame));
    bytes[1] = DUCKNNG_RPC_RESULT;
    prop_write_le32(bytes + 2, 0);
    ASSERT_EQ(-1, ducknng_decode_frame_bytes(bytes, written, &frame));
    prop_write_le32(bytes + 10, 0);
    prop_write_le64(bytes + 14, sizeof(payload));
    ASSERT_EQ(0, ducknng_decode_frame_bytes(bytes, written, &frame));
    ASSERT_EQ(0, ducknng_frame_name_equals(&frame, "x"));
    ASSERT_EQ(1, ducknng_frame_name_equals(&frame, "n"));
    ASSERT_EQ(0, ducknng_frame_name_equals(NULL, "n"));
    ASSERT_EQ(0, ducknng_frame_name_equals(&frame, NULL));
    ASSERT_EQ(0, ducknng_frame_name_equals(&frame, "long"));

    ASSERT_EQ(-1, ducknng_upload_append_parse_prefix(NULL, 0,
        &session_id, &token, &token_len, &quack_offset));
    ASSERT_EQ(-1, ducknng_upload_append_parse_prefix(prefix, 9,
        &session_id, &token, &token_len, &quack_offset));
    prefix[8] = 0;
    prefix[9] = 0;
    ASSERT_EQ(-1, ducknng_upload_append_parse_prefix(prefix, 10,
        &session_id, &token, &token_len, &quack_offset));
    prefix[8] = 1;
    prefix[9] = 1;
    ASSERT_EQ(-1, ducknng_upload_append_parse_prefix(prefix, 10,
        &session_id, &token, &token_len, &quack_offset));
    prefix[8] = 2;
    prefix[9] = 0;
    ASSERT_EQ(-1, ducknng_upload_append_parse_prefix(prefix, 11,
        &session_id, &token, &token_len, &quack_offset));
    prefix[8] = 1;
    prefix[9] = 0;
    prefix[10] = 't';
    ASSERT_EQ(0, ducknng_upload_append_parse_prefix(prefix, 11,
        NULL, NULL, NULL, NULL));
    PASS();
}

TEST transport_rejects_or_classifies_random_urls(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("transport random urls", prop_transport_random_urls, &prop_random_url_info));
    PASS();
}

TEST transport_known_schemes(void)
{
    static const struct {
        const char *url;
        ducknng_transport_family family;
        ducknng_transport_scheme scheme;
        int uses_tls;
    } cases[] = {
        {"inproc://prop", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_INPROC, 0},
        {"ipc:///tmp/ducknng-prop.ipc", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_IPC, 0},
        {"tcp://127.0.0.1:1234", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_TCP, 0},
        {"tls+tcp://127.0.0.1:1234", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_TLS_TCP, 1},
        {"ws://127.0.0.1:1234/rpc", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_WS, 0},
        {"wss://127.0.0.1:1234/rpc", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_WSS, 1},
        {"http://127.0.0.1:1234/rpc", DUCKNNG_TRANSPORT_FAMILY_HTTP, DUCKNNG_TRANSPORT_SCHEME_HTTP, 0},
        {"https://127.0.0.1:1234/rpc", DUCKNNG_TRANSPORT_FAMILY_HTTP, DUCKNNG_TRANSPORT_SCHEME_HTTPS, 1},
    };
    size_t i;

    for (i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        ducknng_transport_url parsed;
        char *errmsg = NULL;

        ASSERT_EQ(0, ducknng_transport_url_parse(cases[i].url, &parsed, &errmsg));
        ASSERT_EQ(cases[i].family, parsed.family);
        ASSERT_EQ(cases[i].scheme, parsed.scheme);
        ASSERT_EQ(cases[i].uses_tls, parsed.uses_tls);
        ASSERT_EQ(NULL, errmsg);
    }
    PASS();
}

TEST quack_rejects_or_scans_random_zero_column_payloads(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("quack random zero-column payloads", prop_quack_random_payloads,
            &prop_random_bytes_info));
    PASS();
}

TEST quack_accepts_compressed_vector_fixtures(void)
{
    struct prop_quack_buf payload;
    ducknng_quack_schema schema;
    idx_t row_count = 0;
    char *errmsg = NULL;

    ASSERT_EQ(0, prop_quack_build_one_col_schema(&schema, PROP_QK_BIGINT));
    ASSERT_EQ(0, prop_quack_payload_constant(&payload));
    ASSERT_EQ(0, ducknng_quack_payload_read_row_count(payload.data, payload.len,
        &schema, &row_count, &errmsg));
    ASSERT_EQ((idx_t)4, row_count);
    ASSERT_EQ(NULL, errmsg);
    ASSERT_EQ(0, prop_quack_payload_sequence(&payload));
    row_count = 0;
    ASSERT_EQ(0, ducknng_quack_payload_read_row_count(payload.data, payload.len,
        &schema, &row_count, &errmsg));
    ASSERT_EQ((idx_t)4, row_count);
    ASSERT_EQ(NULL, errmsg);
    ducknng_quack_schema_reset(&schema);

    ASSERT_EQ(0, prop_quack_build_one_col_schema(&schema, PROP_QK_VARCHAR));
    ASSERT_EQ(0, prop_quack_payload_dictionary(&payload, 0));
    row_count = 0;
    ASSERT_EQ(0, ducknng_quack_payload_read_row_count(payload.data, payload.len,
        &schema, &row_count, &errmsg));
    ASSERT_EQ((idx_t)5, row_count);
    ASSERT_EQ(NULL, errmsg);
    ducknng_quack_schema_reset(&schema);
    PASS();
}

TEST quack_rejects_non_boolean_markers(void)
{
    struct prop_quack_buf payload;
    ducknng_quack_schema schema;
    idx_t row_count = 0;
    char *errmsg = NULL;

    ASSERT_EQ(0, prop_quack_build_one_col_schema(&schema, PROP_QK_BIGINT));
    ASSERT_EQ(0, prop_quack_payload_invalid_validity_boolean(&payload));
    ASSERT_NEQ(0, ducknng_quack_payload_read_row_count(payload.data,
        payload.len, &schema, &row_count, &errmsg));
    ASSERT(errmsg != NULL);
    if (errmsg) free(errmsg);
    errmsg = NULL;
    row_count = 0;

    ASSERT_EQ(0, prop_quack_payload_invalid_chunk_boolean(&payload));
    ASSERT_NEQ(0, ducknng_quack_payload_read_row_count(payload.data,
        payload.len, &schema, &row_count, &errmsg));
    ASSERT(errmsg != NULL);
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    PASS();
}

TEST quack_rejects_malformed_type_info(void)
{
    struct prop_quack_buf payload;

    /* Presence markers are strict booleans. */
    ASSERT_EQ(0, prop_quack_schema_begin(&payload, PROP_QK_INTEGER, 2));
    ASSERT_EQ(0, prop_quack_schema_finish(&payload));
    ASSERT_EQ(0, prop_quack_schema_rejects(&payload));

    /* Nested and parameterized types require present, complete metadata. */
    ASSERT_EQ(0, prop_quack_schema_begin(&payload, PROP_QK_LIST, 0));
    ASSERT_EQ(0, prop_quack_schema_finish(&payload));
    ASSERT_EQ(0, prop_quack_schema_rejects(&payload));

    /* The logical type and ExtraTypeInfo kind must agree. */
    ASSERT_EQ(0, prop_quack_schema_begin(&payload, PROP_QK_LIST, 1));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_INFO_KIND));
    ASSERT_EQ(0, prop_qb_uleb(&payload, PROP_QK_EXTRA_TYPE_STRUCT));
    ASSERT_EQ(0, prop_qb_field_end(&payload));
    ASSERT_EQ(0, prop_quack_schema_finish(&payload));
    ASSERT_EQ(0, prop_quack_schema_rejects(&payload));

    /* Decimal width is nonzero and scale may not exceed width. */
    ASSERT_EQ(0, prop_quack_schema_begin(&payload, PROP_QK_DECIMAL, 1));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_INFO_KIND));
    ASSERT_EQ(0, prop_qb_uleb(&payload, PROP_QK_EXTRA_TYPE_DECIMAL));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_CHILD));
    ASSERT_EQ(0, prop_qb_uleb(&payload, 0));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_ARRAY_SIZE));
    ASSERT_EQ(0, prop_qb_uleb(&payload, 0));
    ASSERT_EQ(0, prop_qb_field_end(&payload));
    ASSERT_EQ(0, prop_quack_schema_finish(&payload));
    ASSERT_EQ(0, prop_quack_schema_rejects(&payload));

    /* MAP requires the serialized LIST child to be STRUCT(key, value). */
    ASSERT_EQ(0, prop_quack_schema_begin(&payload, PROP_QK_MAP, 1));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_INFO_KIND));
    ASSERT_EQ(0, prop_qb_uleb(&payload, PROP_QK_EXTRA_TYPE_LIST));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_CHILD));
    ASSERT_EQ(0, prop_quack_put_primitive_type(&payload, PROP_QK_INTEGER));
    ASSERT_EQ(0, prop_qb_field_end(&payload));
    ASSERT_EQ(0, prop_quack_schema_finish(&payload));
    ASSERT_EQ(0, prop_quack_schema_rejects(&payload));

    /* ARRAY requires exactly one child and a nonzero fixed size. */
    ASSERT_EQ(0, prop_quack_schema_begin(&payload, PROP_QK_ARRAY, 1));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_INFO_KIND));
    ASSERT_EQ(0, prop_qb_uleb(&payload, PROP_QK_EXTRA_TYPE_ARRAY));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_CHILD));
    ASSERT_EQ(0, prop_quack_put_primitive_type(&payload, PROP_QK_INTEGER));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_ARRAY_SIZE));
    ASSERT_EQ(0, prop_qb_uleb(&payload, 0));
    ASSERT_EQ(0, prop_qb_field_end(&payload));
    ASSERT_EQ(0, prop_quack_schema_finish(&payload));
    ASSERT_EQ(0, prop_quack_schema_rejects(&payload));

    /* Duplicate type-info fields are not last-write-wins. */
    ASSERT_EQ(0, prop_quack_schema_begin(&payload, PROP_QK_LIST, 1));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_INFO_KIND));
    ASSERT_EQ(0, prop_qb_uleb(&payload, PROP_QK_EXTRA_TYPE_LIST));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_INFO_KIND));
    ASSERT_EQ(0, prop_qb_uleb(&payload, PROP_QK_EXTRA_TYPE_LIST));
    ASSERT_EQ(0, prop_qb_field_end(&payload));
    ASSERT_EQ(0, prop_quack_schema_finish(&payload));
    ASSERT_EQ(0, prop_quack_schema_rejects(&payload));
    PASS();
}

TEST quack_rejects_malformed_dictionary_fixture(void)
{
    struct prop_quack_buf payload;
    ducknng_quack_schema schema;
    idx_t row_count = 0;
    char *errmsg = NULL;

    ASSERT_EQ(0, prop_quack_build_one_col_schema(&schema, PROP_QK_VARCHAR));
    ASSERT_EQ(0, prop_quack_payload_dictionary(&payload, 1));
    ASSERT_NEQ(0, ducknng_quack_payload_read_row_count(payload.data, payload.len,
        &schema, &row_count, &errmsg));
    ASSERT_EQ((idx_t)0, row_count);
    ASSERT(errmsg != NULL);
    ASSERT(strstr(errmsg, "selection index") != NULL);
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    PASS();
}

TEST quack_rejects_fixed_width_size_overflow_fixture(void)
{
    struct prop_quack_buf payload;
    ducknng_quack_schema schema;
    idx_t row_count = 0;
    char *errmsg = NULL;
    int rc;

    ASSERT_EQ(0, prop_quack_build_one_col_schema(&schema, PROP_QK_BIGINT));
    ASSERT_EQ(0, prop_quack_payload_fixed_width_overflow(&payload));
    rc = ducknng_quack_payload_read_row_count(payload.data, payload.len, &schema,
        &row_count, &errmsg);
    ASSERT_NEQ(0, rc);
    ASSERT_EQ((idx_t)0, row_count);
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    PASS();
}

TEST quack_rejects_blob_length_wraparound_fixture(void)
{
    struct prop_quack_buf payload;
    ducknng_quack_schema schema;
    idx_t row_count = 0;
    char *errmsg = NULL;
    int rc;

    ASSERT_EQ(0, prop_quack_build_one_col_schema(&schema, PROP_QK_VARCHAR));
    ASSERT_EQ(0, prop_quack_payload_varlen_wraparound(&payload));
    rc = ducknng_quack_payload_read_row_count(payload.data, payload.len, &schema,
        &row_count, &errmsg);
    ASSERT_NEQ(0, rc);
    ASSERT_EQ((idx_t)0, row_count);
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    PASS();
}

TEST quack_rejects_huge_schema_column_count_fixture(void)
{
    struct prop_quack_buf payload;
    ducknng_quack_schema schema;
    struct _duckdb_bind_info bind_info;
    idx_t row_count = 0;
    char *errmsg = NULL;
    int rc;

    memset(&schema, 0, sizeof(schema));
    memset(&bind_info, 0, sizeof(bind_info));
    ASSERT_EQ(0, prop_quack_payload_huge_schema_column_count(&payload));
    rc = ducknng_quack_payload_bind_columns((duckdb_bind_info)&bind_info,
        payload.data, payload.len, &schema, &row_count, &errmsg);
    ASSERT_NEQ(0, rc);
    ASSERT_EQ((idx_t)0, row_count);
    ASSERT_EQ((idx_t)0, schema.ncols);
    ASSERT_EQ(NULL, schema.cols);
    ASSERT(errmsg != NULL);
    ASSERT(strstr(errmsg, "column count") != NULL);
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    PASS();
}

TEST quack_rejects_random_nested_schema_payloads(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("quack random nested-schema payloads", prop_quack_nested_random_payloads,
            &prop_random_bytes_info));
    PASS();
}

TEST quack_parse_schema_rejects_random_payloads(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("quack parse_schema random payloads", prop_quack_parse_schema_random_payloads,
            &prop_random_bytes_info));
    PASS();
}

/* A column name carrying an embedded NUL must be rejected by parse_schema, so a
 * counted name like "a\0x" cannot strcmp-match target column "a" on the upload
 * path. The NUL-free control proves the fixture is otherwise valid and the
 * guard is not over-broad. */
TEST quack_parse_schema_rejects_embedded_nul_column_name(void)
{
    static const uint8_t nul_name[3] = { 'a', '\0', 'x' };
    static const uint8_t ok_name[2] = { 'a', 'x' };
    struct prop_quack_buf b;
    ducknng_quack_schema schema;
    char *errmsg = NULL;
    int rc;

    /* Negative: embedded-NUL name is rejected, nothing is left allocated. */
    ASSERT_EQ(0, prop_quack_build_one_col_named_schema(&b, nul_name, sizeof(nul_name)));
    memset(&schema, 0, sizeof(schema));
    rc = ducknng_quack_payload_parse_schema(b.data, b.len, &schema, &errmsg);
    ASSERT_NEQ(0, rc);
    ASSERT(errmsg != NULL);
    ASSERT(strstr(errmsg, "embedded NUL") != NULL);
    ASSERT_EQ((idx_t)0, schema.ncols);
    ASSERT_EQ(NULL, schema.cols);
    if (errmsg) { free(errmsg); errmsg = NULL; }
    ducknng_quack_schema_reset(&schema);

    /* Positive control: the same shape with a NUL-free name parses cleanly. */
    ASSERT_EQ(0, prop_quack_build_one_col_named_schema(&b, ok_name, sizeof(ok_name)));
    memset(&schema, 0, sizeof(schema));
    rc = ducknng_quack_payload_parse_schema(b.data, b.len, &schema, &errmsg);
    ASSERT_EQ(0, rc);
    ASSERT_EQ(NULL, errmsg);
    ASSERT_EQ((idx_t)1, schema.ncols);
    ASSERT(schema.cols != NULL);
    ASSERT(schema.cols[0].name != NULL);
    ASSERT_EQ(0, strcmp(schema.cols[0].name, "ax"));
    ducknng_quack_schema_reset(&schema);
    PASS();
}

TEST size_add_rejects_overflow_keeps_valid_sums(void)
{
    size_t out = 0;

    ASSERT_EQ(0, ducknng_size_add(10, 20, &out));
    ASSERT_EQ((size_t)30, out);
    ASSERT_EQ(0, ducknng_size_add(10, 20, NULL));

    /* Exact boundary: sum equals SIZE_MAX is representable. */
    out = 0;
    ASSERT_EQ(0, ducknng_size_add(SIZE_MAX - 1, 1, &out));
    ASSERT_EQ(SIZE_MAX, out);

    /* One past the boundary overflows and must not write a wrapped value. */
    out = 0xabcd;
    ASSERT_EQ(-1, ducknng_size_add(SIZE_MAX, 1, &out));
    ASSERT_EQ((size_t)0xabcd, out);

    out = 0xabcd;
    ASSERT_EQ(-1, ducknng_size_add(SIZE_MAX - 4, 5, &out));
    ASSERT_EQ((size_t)0xabcd, out);
    PASS();
}

TEST size_mul_rejects_overflow_keeps_valid_products(void)
{
    size_t out = 0xabcd;

    ASSERT_EQ(0, ducknng_size_mul(6, 7, &out));
    ASSERT_EQ((size_t)42, out);
    ASSERT_EQ(0, ducknng_size_mul(6, 7, NULL));

    /* Zero operand can never overflow. */
    out = 0xabcd;
    ASSERT_EQ(0, ducknng_size_mul(0, SIZE_MAX, &out));
    ASSERT_EQ((size_t)0, out);

    out = 0xabcd;
    ASSERT_EQ(-1, ducknng_size_mul(SIZE_MAX, 2, &out));
    ASSERT_EQ((size_t)0xabcd, out);

    out = 0xabcd;
    ASSERT_EQ(-1, ducknng_size_mul((SIZE_MAX / 4) + 1, 4, &out));
    ASSERT_EQ((size_t)0xabcd, out);
    PASS();
}

TEST grow_capacity_meets_need_without_overflow(void)
{
    size_t cap = 0;

    /* First growth seeds from min_cap, then doubles to cover need. */
    ASSERT_EQ(0, ducknng_grow_capacity(1, 0, 256, &cap));
    ASSERT_EQ((size_t)256, cap);

    ASSERT_EQ(0, ducknng_grow_capacity(300, 256, 256, &cap));
    ASSERT(cap >= 300);
    ASSERT_EQ((size_t)512, cap);

    /* A need already satisfied returns the current capacity unchanged. */
    ASSERT_EQ(0, ducknng_grow_capacity(100, 256, 256, &cap));
    ASSERT_EQ((size_t)256, cap);

    /* Near SIZE_MAX, doubling would overflow; the helper clamps to need
     * instead of wrapping, and never returns a capacity below need. */
    ASSERT_EQ(0, ducknng_grow_capacity(SIZE_MAX, SIZE_MAX / 2 + 8, 256, &cap));
    ASSERT(cap >= SIZE_MAX - 1);
    ASSERT_EQ(SIZE_MAX, cap);
    PASS();
}

TEST size_arith_invariants_hold_for_random_pairs(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("size arithmetic invariants", prop_size_arith_invariants,
            &prop_two_sizes_info));
    PASS();
}

/* The fixed-width variants back idx_t row and element math, which is uint64_t
 * on every target regardless of size_t, so they need their own boundary cases
 * rather than inheriting the size_t ones. */
TEST u64_arith_rejects_overflow_keeps_valid_results(void)
{
    uint64_t out = 0;

    ASSERT_EQ(0, ducknng_u64_add(10u, 20u, &out));
    ASSERT_EQ((uint64_t)30u, out);
    ASSERT_EQ(0, ducknng_u64_add(10u, 20u, NULL));

    out = 0;
    ASSERT_EQ(0, ducknng_u64_add(UINT64_MAX - 1u, 1u, &out));
    ASSERT_EQ(UINT64_MAX, out);

    out = 0xabcdu;
    ASSERT_EQ(-1, ducknng_u64_add(UINT64_MAX, 1u, &out));
    ASSERT_EQ((uint64_t)0xabcdu, out);

    out = 0xabcdu;
    ASSERT_EQ(-1, ducknng_u64_add(UINT64_MAX - 4u, 5u, &out));
    ASSERT_EQ((uint64_t)0xabcdu, out);

    out = 0;
    ASSERT_EQ(0, ducknng_u64_mul(6u, 7u, &out));
    ASSERT_EQ((uint64_t)42u, out);
    ASSERT_EQ(0, ducknng_u64_mul(6u, 7u, NULL));

    /* Zero short-circuits the divide guard in both operand positions. */
    out = 0xabcdu;
    ASSERT_EQ(0, ducknng_u64_mul(0u, UINT64_MAX, &out));
    ASSERT_EQ((uint64_t)0u, out);
    out = 0xabcdu;
    ASSERT_EQ(0, ducknng_u64_mul(UINT64_MAX, 0u, &out));
    ASSERT_EQ((uint64_t)0u, out);

    out = 0;
    ASSERT_EQ(0, ducknng_u64_mul(UINT64_MAX, 1u, &out));
    ASSERT_EQ(UINT64_MAX, out);

    out = 0xabcdu;
    ASSERT_EQ(-1, ducknng_u64_mul(UINT64_MAX, 2u, &out));
    ASSERT_EQ((uint64_t)0xabcdu, out);

    out = 0xabcdu;
    ASSERT_EQ(-1, ducknng_u64_mul((UINT64_MAX >> 1) + 1u, 2u, &out));
    ASSERT_EQ((uint64_t)0xabcdu, out);
    PASS();
}

SUITE(size_checked_properties)
{
    RUN_TEST(size_add_rejects_overflow_keeps_valid_sums);
    RUN_TEST(size_mul_rejects_overflow_keeps_valid_products);
    RUN_TEST(u64_arith_rejects_overflow_keeps_valid_results);
    RUN_TEST(grow_capacity_meets_need_without_overflow);
    RUN_TEST(size_arith_invariants_hold_for_random_pairs);
}

TEST qk_core_rejects_invalid_booleans_and_integers(void)
{
    static const uint8_t yes[] = {1u};
    static const uint8_t invalid_bool[] = {2u};
    static const uint8_t truncated_uleb[] = {0x80u};
    static const uint8_t overflow_uleb[] = {
        0x80u, 0x80u, 0x80u, 0x80u, 0x80u,
        0x80u, 0x80u, 0x80u, 0x80u, 0x02u
    };
    ducknng_qk_reader reader;
    ducknng_qk_writer writer;
    uint8_t value = 0;
    uint64_t integer = 0;

    ducknng_qk_reader_init(&reader, yes, sizeof(yes));
    ASSERT_EQ(0, ducknng_qk_read_boolean(&reader, &value));
    ASSERT_EQ((uint8_t)1, value);
    ASSERT_EQ((size_t)0, ducknng_qk_reader_remaining(&reader));

    ducknng_qk_reader_init(&reader, invalid_bool, sizeof(invalid_bool));
    ASSERT_EQ(-1, ducknng_qk_read_boolean(&reader, &value));
    ASSERT_EQ(DUCKNNG_QK_INVALID, ducknng_qk_reader_status(&reader));

    ducknng_qk_reader_init(&reader, truncated_uleb, sizeof(truncated_uleb));
    ASSERT_EQ(-1, ducknng_qk_read_uleb128(&reader, &integer));
    ASSERT_EQ(DUCKNNG_QK_TRUNCATED, ducknng_qk_reader_status(&reader));

    ducknng_qk_reader_init(&reader, overflow_uleb, sizeof(overflow_uleb));
    ASSERT_EQ(-1, ducknng_qk_read_uleb128(&reader, &integer));
    ASSERT_EQ(DUCKNNG_QK_OVERFLOW, ducknng_qk_reader_status(&reader));

    ducknng_qk_writer_init_fixed(&writer, NULL, 1);
    ASSERT_EQ(DUCKNNG_QK_INVALID, ducknng_qk_writer_status(&writer));
    ASSERT_EQ(-1, ducknng_qk_write_u8(&writer, 1));
    PASS();
}

TEST qk_core_roundtrips_random_integers(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("quack core integer roundtrip",
            prop_qk_core_integer_roundtrip, &prop_two_sizes_info));
    PASS();
}

TEST qk_core_exercises_explicit_api_edges(void)
{
    static const uint8_t sleb_minus_one[] = {0x7fu};
    static const uint8_t leb_shift_overflow[] = {
        0x80u, 0x80u, 0x80u, 0x80u, 0x80u,
        0x80u, 0x80u, 0x80u, 0x80u, 0x80u, 0x00u
    };
    static const uint8_t sleb_payload_overflow[] = {
        0x80u, 0x80u, 0x80u, 0x80u, 0x80u,
        0x80u, 0x80u, 0x80u, 0x80u, 0x01u
    };
    uint8_t bytes[32];
    uint8_t value = 0;
    uint16_t field = 0;
    uint64_t unsigned_value = 0;
    int64_t signed_value = 0;
    const uint8_t *counted = NULL;
    size_t counted_size = 0;
    ducknng_qk_writer writer;
    ducknng_qk_reader reader;

    ducknng_qk_writer_init_measure(NULL);
    ducknng_qk_writer_init_fixed(&writer, NULL, 0);
    ASSERT_EQ(DUCKNNG_QK_OK, ducknng_qk_writer_status(&writer));
    ASSERT_EQ((size_t)0, ducknng_qk_writer_size(NULL));
    ASSERT_EQ(DUCKNNG_QK_INVALID, ducknng_qk_writer_status(NULL));
    ASSERT_EQ(-1, ducknng_qk_write_u8(NULL, 1));

    ducknng_qk_writer_init_measure(&writer);
    ASSERT_EQ(0, ducknng_qk_write_bytes(&writer, NULL, 0));
    ASSERT_EQ(-1, ducknng_qk_write_bytes(&writer, NULL, 1));
    ASSERT_EQ(DUCKNNG_QK_INVALID, ducknng_qk_writer_status(&writer));
    ASSERT_EQ(-1, ducknng_qk_write_u8(&writer, 1));

    ducknng_qk_writer_init_measure(&writer);
    writer.position = SIZE_MAX;
    ASSERT_EQ(-1, ducknng_qk_write_u8(&writer, 1));
    ASSERT_EQ(DUCKNNG_QK_OVERFLOW, ducknng_qk_writer_status(&writer));

    memset(&writer, 0, sizeof(writer));
    ASSERT_EQ(-1, ducknng_qk_write_u8(&writer, 1));
    ASSERT_EQ(DUCKNNG_QK_INVALID, ducknng_qk_writer_status(&writer));

    ducknng_qk_writer_init_fixed(&writer, bytes, sizeof(bytes));
    ASSERT_EQ(0, ducknng_qk_write_u16le(&writer, 0x1234u));
    ASSERT_EQ(0, ducknng_qk_write_field(&writer, 0xabcd));
    ASSERT_EQ(0, ducknng_qk_write_sleb128(&writer, INT64_MIN));
    ASSERT(ducknng_qk_writer_size(&writer) <= sizeof(bytes));

    ducknng_qk_reader_init(NULL, NULL, 0);
    ASSERT_EQ((size_t)0, ducknng_qk_reader_remaining(NULL));
    ASSERT_EQ(DUCKNNG_QK_INVALID, ducknng_qk_reader_status(NULL));
    ASSERT_EQ(-1, ducknng_qk_read_u8(NULL, &value));
    ducknng_qk_reader_init(&reader, NULL, 1);
    ASSERT_EQ(DUCKNNG_QK_INVALID, ducknng_qk_reader_status(&reader));
    ASSERT_EQ(-1, ducknng_qk_read_u8(&reader, &value));

    ducknng_qk_reader_init(&reader, bytes, 1);
    reader.off = 2;
    ASSERT_EQ((size_t)0, ducknng_qk_reader_remaining(&reader));
    ASSERT_EQ(-1, ducknng_qk_skip(&reader, 0));
    ASSERT_EQ(DUCKNNG_QK_TRUNCATED, ducknng_qk_reader_status(&reader));

    { static const uint8_t two[] = {0x34u, 0x12u};
      ducknng_qk_reader_init(&reader, two, sizeof(two));
      ASSERT_EQ(0, ducknng_qk_peek_u16le(&reader, &field));
      ASSERT_EQ((uint16_t)0x1234, field);
      ASSERT_EQ(0, ducknng_qk_read_u16le(&reader, NULL));
      ASSERT_EQ((size_t)0, ducknng_qk_reader_remaining(&reader)); }

    ducknng_qk_reader_init(&reader, sleb_minus_one, sizeof(sleb_minus_one));
    ASSERT_EQ(0, ducknng_qk_read_sleb128(&reader, &signed_value));
    ASSERT_EQ((int64_t)-1, signed_value);

    ducknng_qk_reader_init(&reader, leb_shift_overflow,
        sizeof(leb_shift_overflow));
    ASSERT_EQ(-1, ducknng_qk_read_uleb128(&reader, &unsigned_value));
    ASSERT_EQ(DUCKNNG_QK_OVERFLOW, ducknng_qk_reader_status(&reader));
    ducknng_qk_reader_init(&reader, leb_shift_overflow,
        sizeof(leb_shift_overflow));
    ASSERT_EQ(-1, ducknng_qk_read_sleb128(&reader, &signed_value));
    ASSERT_EQ(DUCKNNG_QK_OVERFLOW, ducknng_qk_reader_status(&reader));
    ducknng_qk_reader_init(&reader, sleb_payload_overflow,
        sizeof(sleb_payload_overflow));
    ASSERT_EQ(-1, ducknng_qk_read_sleb128(&reader, &signed_value));
    ASSERT_EQ(DUCKNNG_QK_OVERFLOW, ducknng_qk_reader_status(&reader));

    { static const uint8_t counted_value[] = {2u, 'o', 'k'};
      ducknng_qk_reader_init(&reader, counted_value, sizeof(counted_value));
      ASSERT_EQ(0, ducknng_qk_read_counted(&reader, NULL, NULL));
      ASSERT_EQ((size_t)0, ducknng_qk_reader_remaining(&reader));
      ducknng_qk_reader_init(&reader, counted_value, 2);
      ASSERT_EQ(-1, ducknng_qk_read_counted(&reader, &counted, &counted_size));
      ASSERT_EQ(DUCKNNG_QK_TRUNCATED, ducknng_qk_reader_status(&reader)); }
    PASS();
}

TEST qk_type_core_exercises_structural_contracts(void)
{
    ducknng_quack_column_schema a;
    ducknng_quack_column_schema b;
    ducknng_quack_column_schema child;
    ducknng_quack_column_schema tag;
    ducknng_quack_column_schema entry;
    ducknng_quack_column_schema value;
    ducknng_quack_column_schema *one_child[1];
    ducknng_quack_column_schema *two_children[2];
    char *one_name[1];
    char *two_names[2];
    char *labels_a[2];
    char *labels_b[2];
    const char *message = NULL;

    ASSERT_EQ((size_t)0, ducknng_qk_validity_bytes(0));
    ASSERT_EQ((size_t)8, ducknng_qk_validity_bytes(1));
    ASSERT_EQ((size_t)8, ducknng_qk_validity_bytes(64));
    ASSERT_EQ((size_t)16, ducknng_qk_validity_bytes(65));
    ASSERT_EQ(SIZE_MAX, ducknng_qk_validity_bytes(UINT64_MAX));

    memset(&a, 0, sizeof(a));
    memset(&b, 0, sizeof(b));
    memset(&child, 0, sizeof(child));
    memset(&tag, 0, sizeof(tag));
    memset(&entry, 0, sizeof(entry));
    memset(&value, 0, sizeof(value));
    child.logical_type_id = DUCKNNG_QUACK_LOGICAL_INTEGER;
    value.logical_type_id = DUCKNNG_QUACK_LOGICAL_VARCHAR;
    tag.logical_type_id = DUCKNNG_QUACK_LOGICAL_UTINYINT;
    one_child[0] = &child;
    one_name[0] = "x";
    two_children[0] = &tag;
    two_children[1] = &child;
    two_names[0] = "";
    two_names[1] = "x";

    ASSERT_EQ(0, ducknng_qk_type_is_nested(NULL));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_LIST;
    ASSERT_EQ(1, ducknng_qk_type_is_nested(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_MAP;
    ASSERT_EQ(1, ducknng_qk_type_is_nested(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_STRUCT;
    ASSERT_EQ(1, ducknng_qk_type_is_nested(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_UNION;
    ASSERT_EQ(1, ducknng_qk_type_is_nested(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_ARRAY;
    ASSERT_EQ(1, ducknng_qk_type_is_nested(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_INTEGER;
    ASSERT_EQ(0, ducknng_qk_type_is_nested(&a));

    ASSERT_EQ(0, ducknng_qk_type_is_varlen(NULL));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_VARCHAR;
    ASSERT_EQ(1, ducknng_qk_type_is_varlen(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_CHAR;
    ASSERT_EQ(1, ducknng_qk_type_is_varlen(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_BLOB;
    ASSERT_EQ(1, ducknng_qk_type_is_varlen(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_INTEGER;
    ASSERT_EQ(0, ducknng_qk_type_is_varlen(&a));

    ASSERT_EQ((size_t)0, ducknng_qk_type_fixed_width(NULL));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_BOOLEAN;
    ASSERT_EQ((size_t)1, ducknng_qk_type_fixed_width(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_SMALLINT;
    ASSERT_EQ((size_t)2, ducknng_qk_type_fixed_width(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_INTEGER;
    ASSERT_EQ((size_t)4, ducknng_qk_type_fixed_width(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_BIGINT;
    ASSERT_EQ((size_t)8, ducknng_qk_type_fixed_width(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_UUID;
    ASSERT_EQ((size_t)16, ducknng_qk_type_fixed_width(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_DECIMAL;
    a.decimal_width = 4;
    ASSERT_EQ((size_t)2, ducknng_qk_type_fixed_width(&a));
    a.decimal_width = 9;
    ASSERT_EQ((size_t)4, ducknng_qk_type_fixed_width(&a));
    a.decimal_width = 18;
    ASSERT_EQ((size_t)8, ducknng_qk_type_fixed_width(&a));
    a.decimal_width = 38;
    ASSERT_EQ((size_t)16, ducknng_qk_type_fixed_width(&a));
    a.decimal_width = 39;
    ASSERT_EQ((size_t)0, ducknng_qk_type_fixed_width(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_ENUM;
    a.enum_count = 0xffu;
    ASSERT_EQ((size_t)1, ducknng_qk_type_fixed_width(&a));
    a.enum_count = 0xffffu;
    ASSERT_EQ((size_t)2, ducknng_qk_type_fixed_width(&a));
    a.enum_count = 0x10000u;
    ASSERT_EQ((size_t)4, ducknng_qk_type_fixed_width(&a));
    a.logical_type_id = -1;
    ASSERT_EQ((size_t)0, ducknng_qk_type_fixed_width(&a));

    ASSERT_EQ(0, ducknng_qk_type_is_sequence_integer(NULL));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_TINYINT;
    ASSERT_EQ(1, ducknng_qk_type_is_sequence_integer(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_UBIGINT;
    ASSERT_EQ(1, ducknng_qk_type_is_sequence_integer(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_DOUBLE;
    ASSERT_EQ(0, ducknng_qk_type_is_sequence_integer(&a));

    ASSERT_EQ(DUCKNNG_QUACK_EXTRA_TYPE_DECIMAL,
        ducknng_qk_type_expected_info_kind(DUCKNNG_QUACK_LOGICAL_DECIMAL));
    ASSERT_EQ(DUCKNNG_QUACK_EXTRA_TYPE_LIST,
        ducknng_qk_type_expected_info_kind(DUCKNNG_QUACK_LOGICAL_LIST));
    ASSERT_EQ(DUCKNNG_QUACK_EXTRA_TYPE_LIST,
        ducknng_qk_type_expected_info_kind(DUCKNNG_QUACK_LOGICAL_MAP));
    ASSERT_EQ(DUCKNNG_QUACK_EXTRA_TYPE_STRUCT,
        ducknng_qk_type_expected_info_kind(DUCKNNG_QUACK_LOGICAL_STRUCT));
    ASSERT_EQ(DUCKNNG_QUACK_EXTRA_TYPE_STRUCT,
        ducknng_qk_type_expected_info_kind(DUCKNNG_QUACK_LOGICAL_UNION));
    ASSERT_EQ(DUCKNNG_QUACK_EXTRA_TYPE_ENUM,
        ducknng_qk_type_expected_info_kind(DUCKNNG_QUACK_LOGICAL_ENUM));
    ASSERT_EQ(DUCKNNG_QUACK_EXTRA_TYPE_ARRAY,
        ducknng_qk_type_expected_info_kind(DUCKNNG_QUACK_LOGICAL_ARRAY));
    ASSERT_EQ(UINT64_MAX, ducknng_qk_type_expected_info_kind(-1));
    ASSERT_EQ(0, ducknng_qk_type_needs_info(NULL));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_LIST;
    ASSERT_EQ(1, ducknng_qk_type_needs_info(&a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_INTEGER;
    ASSERT_EQ(0, ducknng_qk_type_needs_info(&a));

    memset(&a, 0, sizeof(a));
    memset(&b, 0, sizeof(b));
    a.logical_type_id = b.logical_type_id = DUCKNNG_QUACK_LOGICAL_INTEGER;
    ASSERT_EQ(0, ducknng_qk_type_equal(NULL, &b));
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, NULL));
    ASSERT_EQ(1, ducknng_qk_type_equal(&a, &b));
    b.logical_type_id++;
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));
    b = a; b.decimal_width = 1;
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));
    b = a; b.decimal_scale = 1;
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));
    b = a; b.array_size = 1;
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));
    b = a; b.enum_count = 1;
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));
    b = a; b.nchildren = 1;
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));

    memset(&a, 0, sizeof(a)); memset(&b, 0, sizeof(b));
    labels_a[0] = "a"; labels_a[1] = NULL;
    labels_b[0] = "a"; labels_b[1] = NULL;
    a.logical_type_id = b.logical_type_id = DUCKNNG_QUACK_LOGICAL_ENUM;
    a.enum_count = b.enum_count = 2;
    a.enum_labels = labels_a;
    b.enum_labels = labels_b;
    ASSERT_EQ(1, ducknng_qk_type_equal(&a, &b));
    a.enum_labels = NULL;
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));
    a.enum_labels = labels_a; b.enum_labels = NULL;
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));
    b.enum_labels = labels_b; labels_b[0] = "b";
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));
    labels_b[0] = "a";

    memset(&a, 0, sizeof(a)); memset(&b, 0, sizeof(b));
    a.logical_type_id = b.logical_type_id = DUCKNNG_QUACK_LOGICAL_STRUCT;
    a.nchildren = b.nchildren = 1;
    a.children = b.children = one_child;
    a.child_names = b.child_names = one_name;
    ASSERT_EQ(1, ducknng_qk_type_equal(&a, &b));
    a.children = NULL;
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));
    a.children = one_child; b.children = NULL;
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));
    b.children = one_child; a.child_names = NULL; b.child_names = NULL;
    ASSERT_EQ(1, ducknng_qk_type_equal(&a, &b));
    one_name[0] = NULL;
    a.child_names = one_name; b.child_names = NULL;
    ASSERT_EQ(1, ducknng_qk_type_equal(&a, &b));
    a.child_names = NULL; b.child_names = one_name;
    ASSERT_EQ(1, ducknng_qk_type_equal(&a, &b));
    one_name[0] = "x";
    a.child_names = one_name; b.child_names = NULL;
    ASSERT_EQ(0, ducknng_qk_type_equal(&a, &b));
    b.child_names = one_name;

    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(NULL, NULL));
    memset(&a, 0, sizeof(a));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_DECIMAL;
    a.decimal_width = 10; a.decimal_scale = 2;
    ASSERT_EQ(0, ducknng_qk_type_shape_validate(&a, &message));
    a.decimal_width = 0;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.decimal_width = 39; a.decimal_scale = 0;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.decimal_width = 10; a.decimal_scale = 11;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));

    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_LIST;
    a.nchildren = 1; a.children = one_child;
    ASSERT_EQ(0, ducknng_qk_type_shape_validate(&a, &message));
    a.nchildren = 0;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.nchildren = 1; a.children = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.children = one_child; one_child[0] = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    one_child[0] = &child;

    entry.logical_type_id = DUCKNNG_QUACK_LOGICAL_STRUCT;
    entry.nchildren = 2; entry.children = two_children;
    one_child[0] = &entry;
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_MAP;
    a.nchildren = 1; a.children = one_child;
    ASSERT_EQ(0, ducknng_qk_type_shape_validate(&a, &message));
    entry.children = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    entry.children = two_children; entry.nchildren = 1;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    entry.nchildren = 2; entry.logical_type_id = DUCKNNG_QUACK_LOGICAL_LIST;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    entry.logical_type_id = DUCKNNG_QUACK_LOGICAL_STRUCT;

    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_STRUCT;
    a.nchildren = 1; a.children = one_child; a.child_names = one_name;
    one_child[0] = &child;
    ASSERT_EQ(0, ducknng_qk_type_shape_validate(&a, &message));
    a.nchildren = 0;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.nchildren = 1; a.children = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.children = one_child; a.child_names = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.child_names = one_name; one_child[0] = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    one_child[0] = &child; one_name[0] = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    one_name[0] = "x";

    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_UNION;
    a.nchildren = 2; a.children = two_children; a.child_names = two_names;
    ASSERT_EQ(0, ducknng_qk_type_shape_validate(&a, &message));
    a.nchildren = 1;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.nchildren = 2; a.children = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.children = two_children; a.child_names = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.child_names = two_names; two_children[0] = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    two_children[0] = &child;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    two_children[0] = &tag;

    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_ENUM;
    a.enum_count = 0; a.enum_labels = NULL;
    ASSERT_EQ(0, ducknng_qk_type_shape_validate(&a, &message));
    a.enum_count = DUCKNNG_QUACK_MAX_ENUM_VALUES + 1u;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.enum_count = 1;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.enum_labels = labels_a; labels_a[0] = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    labels_a[0] = "a";
    ASSERT_EQ(0, ducknng_qk_type_shape_validate(&a, &message));

    one_child[0] = &child;
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_ARRAY;
    a.array_size = 3; a.nchildren = 1; a.children = one_child;
    ASSERT_EQ(0, ducknng_qk_type_shape_validate(&a, &message));
    a.array_size = 0;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.array_size = 3; a.nchildren = 0;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.nchildren = 1; a.children = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    a.children = one_child; one_child[0] = NULL;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));

    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_VARCHAR;
    ASSERT_EQ(0, ducknng_qk_type_shape_validate(&a, &message));
    a.logical_type_id = DUCKNNG_QUACK_LOGICAL_INTEGER;
    ASSERT_EQ(0, ducknng_qk_type_shape_validate(&a, &message));
    a.logical_type_id = -1;
    ASSERT_EQ(-1, ducknng_qk_type_shape_validate(&a, &message));
    ASSERT(message != NULL);
    PASS();
}

SUITE(quack_byte_core_properties)
{
    RUN_TEST(qk_core_rejects_invalid_booleans_and_integers);
    RUN_TEST(qk_core_roundtrips_random_integers);
    RUN_TEST(qk_core_exercises_explicit_api_edges);
    RUN_TEST(qk_type_core_exercises_structural_contracts);
}

TEST join_dotted_path_handles_edges(void)
{
    char *r;

    r = ducknng_join_dotted_path("", "x");   ASSERT(r); ASSERT_STR_EQ("x", r);   free(r);
    r = ducknng_join_dotted_path(NULL, "x"); ASSERT(r); ASSERT_STR_EQ("x", r);   free(r);
    r = ducknng_join_dotted_path("a", "b");  ASSERT(r); ASSERT_STR_EQ("a.b", r); free(r);
    r = ducknng_join_dotted_path("a.b", "c");ASSERT(r); ASSERT_STR_EQ("a.b.c", r); free(r);
    r = ducknng_join_dotted_path("a", "");   ASSERT(r); ASSERT_STR_EQ("a.", r);  free(r);
    r = ducknng_join_dotted_path("a", NULL); ASSERT(r); ASSERT_STR_EQ("a.", r);  free(r);
    r = ducknng_join_dotted_path("", "");    ASSERT(r); ASSERT_STR_EQ("", r);    free(r);
    PASS();
}

TEST join_dotted_path_invariants_hold_for_random_pairs(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("join dotted path invariants", prop_join_dotted_path_invariants,
            &prop_two_strings_info));
    PASS();
}

SUITE(string_path_properties)
{
    RUN_TEST(join_dotted_path_handles_edges);
    RUN_TEST(join_dotted_path_invariants_hold_for_random_pairs);
}

/* Append one JSON-escaped string literal to a fixed buffer. Returns 0, or -1
 * when the buffer would overflow. */
static int
prop_json_escape_append(char *buf, size_t cap, size_t *len, const char *s)
{
    size_t i;

    if (*len >= cap) return -1;
    buf[(*len)++] = '"';
    for (i = 0; s[i]; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '"' || c == '\\') {
            if (*len + 2 > cap) return -1;
            buf[(*len)++] = '\\';
            buf[(*len)++] = (char)c;
        } else if (c < 0x20) {
            if (*len + 7 > cap) return -1;
            snprintf(buf + *len, cap - *len, "\\u%04x", (unsigned)c);
            *len += 6;
        } else {
            if (*len + 1 > cap) return -1;
            buf[(*len)++] = (char)c;
        }
    }
    if (*len + 1 > cap) return -1;
    buf[(*len)++] = '"';
    return 0;
}

TEST json_string_array_membership_is_exact(void)
{
    size_t count = 0xabcd;
    char *errmsg = NULL;

    /* Exact match admits; substrings, prefixes, and supersets never do. */
    ASSERT_EQ(1, ducknng_json_string_array_contains("[\"alice\"]", "alice", &count, &errmsg));
    ASSERT_EQ((size_t)1, count);
    ASSERT_EQ(NULL, errmsg);
    ASSERT_EQ(0, ducknng_json_string_array_contains("[\"alice-api\"]", "alice", NULL, NULL));
    ASSERT_EQ(0, ducknng_json_string_array_contains("[\"alice\"]", "alice-api", NULL, NULL));
    ASSERT_EQ(0, ducknng_json_string_array_contains("[\"ali\"]", "alice", NULL, NULL));
    ASSERT_EQ(0, ducknng_json_string_array_contains("[\"alice\"]", "lice", NULL, NULL));
    ASSERT_EQ(0, ducknng_json_string_array_contains("[\"alice\"]", "ALICE", NULL, NULL));

    /* Escapes are decoded before comparison. */
    ASSERT_EQ(1, ducknng_json_string_array_contains("[\"a\\\"b\"]", "a\"b", NULL, NULL));
    ASSERT_EQ(1, ducknng_json_string_array_contains("[\"tls:san:spiffe:\\/\\/x\"]",
        "tls:san:spiffe://x", NULL, NULL));

    /* Whitespace and multiple entries are tolerated; count reports entries. */
    ASSERT_EQ(1, ducknng_json_string_array_contains(" [ \"a\" , \"b\" ] ", "b", &count, NULL));
    ASSERT_EQ((size_t)2, count);

    /* Empty array and NULL input contain nothing and are not malformed. */
    ASSERT_EQ(0, ducknng_json_string_array_contains("[]", "a", &count, &errmsg));
    ASSERT_EQ((size_t)0, count);
    ASSERT_EQ(NULL, errmsg);
    ASSERT_EQ(0, ducknng_json_string_array_contains(NULL, "a", &count, &errmsg));
    ASSERT_EQ(NULL, errmsg);

    /* Anything but a single array of strings is malformed and fails closed. */
    {
        static const char *bad[] = {
            "{\"a\":1}", "[1]", "[\"a\"", "[\"a\",]", "[\"a\" \"b\"]",
            "[[\"a\"]]", "[\"a\"]x", "\"a\"", "[null]"
        };
        size_t i;
        for (i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            errmsg = NULL;
            ASSERT_EQ(-1, ducknng_json_string_array_contains(bad[i], "a", &count, &errmsg));
            ASSERT(errmsg != NULL);
            free(errmsg);
        }
    }
    PASS();
}

/* Random byte streams sliced into entries: membership through the JSON parser
 * must agree with plain strcmp membership over the same entries, and a probe
 * string absent from the entry set must never be admitted. */
static enum theft_trial_res
prop_json_array_membership_matches_reference(struct theft *t, void *arg1)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)arg1;
    char entries[64][8];
    size_t entry_count = 0;
    char json[8192];
    size_t json_len = 0;
    size_t i;
    size_t parsed_count = 0;
    char probe[16];

    (void)t;
    for (i = 0; i + 3 <= bytes->len && entry_count < 64; i += 3) {
        size_t j;
        for (j = 0; j < 3; j++) {
            unsigned char c = bytes->data[i + j];
            entries[entry_count][j] = c ? (char)c : (char)0x01;
        }
        entries[entry_count][3] = '\0';
        entry_count++;
    }
    json[json_len++] = '[';
    for (i = 0; i < entry_count; i++) {
        if (i > 0) json[json_len++] = ',';
        if (prop_json_escape_append(json, sizeof(json) - 2, &json_len,
                entries[i]) != 0) {
            return THEFT_TRIAL_SKIP;
        }
    }
    json[json_len++] = ']';
    json[json_len] = '\0';

    if (ducknng_json_string_array_contains(json, NULL, &parsed_count, NULL) != 0) {
        return THEFT_TRIAL_FAIL;
    }
    if (parsed_count != entry_count) return THEFT_TRIAL_FAIL;
    for (i = 0; i < entry_count; i++) {
        if (ducknng_json_string_array_contains(json, entries[i], NULL, NULL) != 1) {
            return THEFT_TRIAL_FAIL;
        }
    }
    /* A probe longer than every entry cannot be a member. */
    if (entry_count > 0) {
        snprintf(probe, sizeof(probe), "%s~", entries[0]);
        if (ducknng_json_string_array_contains(json, probe, NULL, NULL) != 0) {
            return THEFT_TRIAL_FAIL;
        }
    }
    return THEFT_TRIAL_PASS;
}

TEST json_string_array_membership_matches_reference_for_random_entries(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("json string array membership matches reference",
            prop_json_array_membership_matches_reference, &prop_random_bytes_info));
    PASS();
}

SUITE(json_subject_array_properties)
{
    RUN_TEST(json_string_array_membership_is_exact);
    RUN_TEST(json_string_array_membership_matches_reference_for_random_entries);
}

SUITE(wire_properties)
{
    RUN_TEST(wire_rejects_or_decodes_random_bytes);
    RUN_TEST(wire_decodes_generated_valid_frames);
    RUN_TEST(wire_status_roundtrips_and_invalid_assignments_reject);
    RUN_TEST(wire_core_exercises_explicit_api_edges);
    RUN_TEST(upload_prefix_parses_valid_and_rejects_short);
    RUN_TEST(upload_prefix_rejects_random_bytes);
}

SUITE(transport_properties)
{
    RUN_TEST(transport_known_schemes);
    RUN_TEST(transport_rejects_or_classifies_random_urls);
}

/* Regression: ExtraTypeInfo fields 200 and 201 were accepted in either order.
 * For ENUM that let field 201 allocate enum_labels for n=0 (comparing against
 * a still-zero count) and field 200 then publish an enum_count of up to 2^24.
 * Shape validation, ducknng_quack_node_free_contents, and
 * duckdb_create_enum_type all walk enum_count entries of that one-slot array.
 * The count is now published only where the labels are allocated, and the
 * reversed order is rejected outright. */
TEST quack_rejects_reordered_enum_type_info(void)
{
    struct prop_quack_buf payload;
    static const char *const labels[2] = {"alpha", "beta"};
    size_t i;

    /* Field 201 first with zero values, then a large field 200 count. */
    ASSERT_EQ(0, prop_quack_schema_begin(&payload, PROP_QK_ENUM, 1));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_INFO_KIND));
    ASSERT_EQ(0, prop_qb_uleb(&payload, PROP_QK_EXTRA_TYPE_ENUM));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_ARRAY_SIZE));
    ASSERT_EQ(0, prop_qb_uleb(&payload, 0));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_CHILD));
    ASSERT_EQ(0, prop_qb_uleb(&payload, 1000000));
    ASSERT_EQ(0, prop_qb_field_end(&payload));
    ASSERT_EQ(0, prop_quack_schema_finish(&payload));
    ASSERT_EQ(0, prop_quack_schema_rejects_with(&payload,
        "field 201 precedes field 200"));

    /* The ordering rule covers every type carrying both fields, not just ENUM. */
    ASSERT_EQ(0, prop_quack_schema_begin(&payload, PROP_QK_ARRAY, 1));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_INFO_KIND));
    ASSERT_EQ(0, prop_qb_uleb(&payload, PROP_QK_EXTRA_TYPE_ARRAY));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_ARRAY_SIZE));
    ASSERT_EQ(0, prop_qb_uleb(&payload, 4));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_CHILD));
    ASSERT_EQ(0, prop_quack_put_primitive_type(&payload, PROP_QK_INTEGER));
    ASSERT_EQ(0, prop_qb_field_end(&payload));
    ASSERT_EQ(0, prop_quack_schema_finish(&payload));
    ASSERT_EQ(0, prop_quack_schema_rejects_with(&payload,
        "field 201 precedes field 200"));

    /* In serializer order the declared count must still match the labels. */
    ASSERT_EQ(0, prop_quack_schema_begin(&payload, PROP_QK_ENUM, 1));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_INFO_KIND));
    ASSERT_EQ(0, prop_qb_uleb(&payload, PROP_QK_EXTRA_TYPE_ENUM));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_CHILD));
    ASSERT_EQ(0, prop_qb_uleb(&payload, 2));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_ARRAY_SIZE));
    ASSERT_EQ(0, prop_qb_uleb(&payload, 1));
    ASSERT_EQ(0, prop_qb_blob(&payload, labels[0], strlen(labels[0])));
    ASSERT_EQ(0, prop_qb_field_end(&payload));
    ASSERT_EQ(0, prop_quack_schema_finish(&payload));
    ASSERT_EQ(0, prop_quack_schema_rejects_with(&payload,
        "enum values disagree with count"));

    /* A well-formed ENUM in serializer order still decodes. */
    ASSERT_EQ(0, prop_quack_schema_begin(&payload, PROP_QK_ENUM, 1));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_INFO_KIND));
    ASSERT_EQ(0, prop_qb_uleb(&payload, PROP_QK_EXTRA_TYPE_ENUM));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_CHILD));
    ASSERT_EQ(0, prop_qb_uleb(&payload, 2));
    ASSERT_EQ(0, prop_qb_u16(&payload, PROP_QK_EXTRA_ARRAY_SIZE));
    ASSERT_EQ(0, prop_qb_uleb(&payload, 2));
    for (i = 0; i < 2; i++) {
        ASSERT_EQ(0, prop_qb_blob(&payload, labels[i], strlen(labels[i])));
    }
    ASSERT_EQ(0, prop_qb_field_end(&payload));
    ASSERT_EQ(0, prop_quack_schema_finish(&payload));
    ASSERT_EQ(0, prop_quack_schema_accepts(&payload));
    PASS();
}

SUITE(quack_properties)
{
    RUN_TEST(quack_rejects_or_scans_random_zero_column_payloads);
    RUN_TEST(quack_accepts_compressed_vector_fixtures);
    RUN_TEST(quack_rejects_non_boolean_markers);
    RUN_TEST(quack_rejects_malformed_type_info);
    RUN_TEST(quack_rejects_reordered_enum_type_info);
    RUN_TEST(quack_rejects_malformed_dictionary_fixture);
    RUN_TEST(quack_rejects_fixed_width_size_overflow_fixture);
    RUN_TEST(quack_rejects_blob_length_wraparound_fixture);
    RUN_TEST(quack_rejects_huge_schema_column_count_fixture);
    RUN_TEST(quack_rejects_random_nested_schema_payloads);
    RUN_TEST(quack_parse_schema_rejects_random_payloads);
    RUN_TEST(quack_parse_schema_rejects_embedded_nul_column_name);
}

GREATEST_MAIN_DEFS();

int
main(int argc, char **argv)
{
    prop_init_duckdb_api();
    GREATEST_MAIN_BEGIN();
    RUN_SUITE(size_checked_properties);
    RUN_SUITE(quack_byte_core_properties);
    RUN_SUITE(string_path_properties);
    RUN_SUITE(json_subject_array_properties);
    RUN_SUITE(wire_properties);
    RUN_SUITE(transport_properties);
    RUN_SUITE(quack_properties);
    GREATEST_MAIN_END();
}
