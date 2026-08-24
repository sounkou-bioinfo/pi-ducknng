#pragma once
#include "duckdb_extension.h"
#include <stddef.h>
#include <stdint.h>

char *ducknng_strdup(const char *s);
char *ducknng_make_temp_dir(const char *prefix);
int ducknng_remove_file(const char *path);
int ducknng_remove_dir(const char *path);
uint64_t ducknng_now_ms(void);
uint64_t ducknng_wall_clock_ms(void);
void ducknng_sleep_ms(uint64_t ms);
int ducknng_ascii_tolower_int(int c);
int ducknng_ascii_ieq(const char *a, const char *b);
int ducknng_ascii_istarts_with(const char *s, const char *prefix);
int ducknng_ascii_iends_with(const char *s, const char *suffix);
int ducknng_http_token_is_valid(const char *s);
int ducknng_http_headers_json_get_header(const char *headers_json, const char *wanted_name,
    int reject_duplicates, char **out_value, char **errmsg);
int ducknng_json_object_get_string(const char *json, const char *wanted_name,
    int ascii_case_insensitive, int reject_duplicates, char **out_value, char **errmsg);
int ducknng_query_string_get_param(const char *query_string, const char *wanted_name,
    int reject_duplicates, char **out_value, char **errmsg);
int ducknng_cookie_header_get_value(const char *cookie_header, const char *wanted_name,
    int reject_duplicates, char **out_value, char **errmsg);
uint16_t ducknng_le16_read(const uint8_t *p);
uint32_t ducknng_le32_read(const uint8_t *p);
uint64_t ducknng_le64_read(const uint8_t *p);
void ducknng_le16_write(uint8_t *p, uint16_t v);
void ducknng_le32_write(uint8_t *p, uint32_t v);
void ducknng_le64_write(uint8_t *p, uint64_t v);
