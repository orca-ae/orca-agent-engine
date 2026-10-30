// Copyright The Orca Authors
// SPDX-License-Identifier: Apache-2.0

/* Static: a package-provided dynamic loader or shell cannot forge this probe. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/capability.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

int main(int argc, char **argv) {
  if (getuid() != 1000 || geteuid() != 1000 || getgid() != 1000 || getegid() != 1000 || getppid() != 1 ||
      prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) != 1) return 70;
  struct __user_cap_header_struct header = { _LINUX_CAPABILITY_VERSION_3, 0 };
  struct __user_cap_data_struct caps[2] = {{0}, {0}};
  if (syscall(SYS_capget, &header, caps)) return 71;
  if (caps[0].effective || caps[1].effective || caps[0].permitted || caps[1].permitted) return 71;
  char self_ns[128] = {0}, init_ns[128] = {0};
  if (readlink("/proc/self/ns/pid", self_ns, sizeof(self_ns) - 1) < 0 ||
      readlink("/proc/1/ns/pid", init_ns, sizeof(init_ns) - 1) < 0 || strcmp(self_ns, init_ns)) return 78;
  for (int cap = 0; cap < 64; cap++) {
    int present = prctl(PR_CAPBSET_READ, cap, 0, 0, 0);
    if (present > 0 || (present < 0 && errno != EINVAL)) return 71;
  }
  struct stat marker;
  if (lstat("/.orca/package-readonly", &marker) || !S_ISREG(marker.st_mode) ||
      marker.st_uid != getuid()) return 72;
  if (chmod("/.orca/package-readonly", 0644) == 0 || errno != EROFS) return 73;
  int fd = open("/.orca/package-readonly", O_WRONLY | O_NOFOLLOW);
  if (fd >= 0) { close(fd); return 74; }
  if (errno != EROFS && errno != EACCES) return 74;
  for (int i = 1; i < argc; i++) {
    char *name = NULL;
    if (asprintf(&name, "%s/.orca-package-probe-XXXXXX", argv[i]) < 0) return 75;
    fd = mkstemp(name);
    if (fd < 0) { free(name); return 76; }
    close(fd);
    if (unlink(name)) { free(name); return 77; }
    free(name);
  }
  puts("orca_package_boundary_ok");
  return 0;
}
