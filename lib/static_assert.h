#ifndef _STATIC_ASSERT_H
#define _STATIC_ASSERT_H

/*
 * From:    http://www.pixelbeat.org/programming/gcc/static_assert.html
 * License: GNU All-Permissive License
 * Date:    October 2017
 */
#define ASSERT_CONCAT_(a, b) a##b
#define ASSERT_CONCAT(a, b) ASSERT_CONCAT_(a, b)
/* These can't be used after statements in c89. */
#ifdef __COUNTER__
  #define STATIC_ASSERT(e,m) \
    ;enum { ASSERT_CONCAT(static_assert_, __COUNTER__) = 1/(int)(!!(e)) }
#else
  /* This can't be used twice on the same line so ensure if using in headers
   * that the headers are not included twice (by wrapping in #ifndef...#endif)
   * Note it doesn't cause an issue when used on same line of separate modules
   * compiled with gcc -combine -fwhole-program.  */
  #define STATIC_ASSERT(e,m) \
    ;enum { ASSERT_CONCAT(assert_line_, __LINE__) = 1/(int)(!!(e)) }
#endif

  // TODO: D4
  // #define STATIC_ASSERTm(COND,MSG) typedef char static_assertion_##MSG[(!!(COND))*2-1]
  // #define __STATIC_ASSERT(X,L) STATIC_ASSERTm(X,static_assertion_at_line_##L)
  // #define _STATIC_ASSERT(X,L) __STATIC_ASSERT(X,L)
  // #define STATIC_ASSERT(X)    _STATIC_ASSERT(X,__LINE__)
      //
      // STATIC_ASSERTm(1, this_should_be_true);
      // STATIC_ASSERT(sizeof(long)==8);
      // int main()
      // {
      //     STATIC_ASSERT(sizeof(int)==4);
      // }
      //


#endif
