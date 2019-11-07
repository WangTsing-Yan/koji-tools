Name:       koji-tools
Version:    1.3
Release:    2%{?dist}
Summary:    A collection of libraries and tools for interacting with Koji.
License:    LGPLv2 GPLv2+
URL:        https://pagure.io/koji-tools
Source0:    %{name}-%{version}.tar.gz
BuildArch:  noarch

BuildRequires:  python-devel
Requires: koji
Requires: openssl

%description
provides a collection of tools/utilities that interacts
and automates koji tasks for the user.

%prep
%setup -q

%build

%install
install -d $RPM_BUILD_ROOT%{_bindir}
install -pm 0755 src/bin/* $RPM_BUILD_ROOT%{_bindir}

%files
%{!?_licensedir:%global license %doc}
%license COPYING LGPL
%{_bindir}/*


%changelog
* Mon Sep 26 2016 Jay Greguske <jgregusk@redhat.com> 1.3-2
- enable rhel 5 builds (jgregusk@redhat.com)
- remove a brew reference (jgregusk@redhat.com)

* Thu Sep 08 2016 Jay Greguske <jgregusk@redhat.com> 1.3-1
- half a dozen more tools made generic (jgregusk@redhat.com)

* Thu Aug 25 2016 Mike McLean <mikem@redhat.com> 1.2-1
- rename to koji-tools

* Thu Aug 25 2016 Jay Greguske <jgregusk@redhat.com> 1.1-1
- add koji-compare-buildroots (jgregusk@redhat.com)

* Thu Aug 25 2016 Jay Greguske <jgregusk@redhat.com> 1.0-1
- new package built with tito

* Thu Aug 25 2016 Jay Greguske <jgregusk@redhat.com> 1.0-1
 Initial Commit
