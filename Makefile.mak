################################################################
# Build the SWI-Prolog ODBC package for MS-Windows
#
# Author: Jan Wielemaker
# 
# Use:
#	nmake /f Makefile.mak
#	nmake /f Makefile.mak install
################################################################

PLHOME=..\..
!include ..\..\src\rules.mk
PKGDLL=odbc

OBJ=		odbc.obj

all:		$(PKGDLL).dll

$(PKGDLL).dll:	$(OBJ)
		$(LD) /dll /out:$@ $(LDFLAGS) $(OBJ) $(PLLIB) $(LIBS)

!IF "$(CFG)" == "rt"
install:	idll
!ELSE
install:	idll ilib
!ENDIF

idll::
		copy $(PKGDLL).dll $(BINDIR)
ilib::
		copy odbc.pl $(PLBASE)\library
		$(MAKEINDEX)

html-install::
		copy odbc.html $(PKGDOC)

uninstall::
		del $(PLBASE)\bin\$(PKGDLL).dll
		del $(PLBASE)\library\odbc.pl
		$(MAKEINDEX)

clean::
		DEL *.obj *~

distclean:	clean
		DEL $(PKGDLL).dll $(PKGDLL).lib $(PKGDLL).exp

