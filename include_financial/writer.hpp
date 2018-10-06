/* ***************************************************************************
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License version 3 as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 *  License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 ****************************************************************************
 *
 * Authors: Tiziano De Matteis <dematteis at di.unipi.it>
 *	    Gabriele Mencagli <mencagli at di.unipi.it>
 */


#if !defined(WRITER_HPP)
#define WRITER_HPP

#include <stdio.h>
#include "general.h"

class Writer {
public:
    Writer():fd(NULL) {}

    bool open(const char dbname[]) {
	fd=fopen(dbname, "w");
	return (fd!=NULL);
    }
    void close() { fclose(fd); }

	bool writeCandle(const winresult_t &res) {
	if (res.type == -1) return false;

	//e.g. save the candlestick info to file
	fprintf(fd,"type=%d,Bid,%Ld,%.3f,%.3f,%.3f,%.3f\n",res.type,res.timestamp,res.open_bid,res.low_bid,res.high_bid,res.close_bid);
	fprintf(fd,"type=%d,Ask,%Ld,%.3f,%.3f,%.3f,%.3f\n",res.type,res.timestamp,res.open_ask,res.low_ask,res.high_ask,res.close_ask);
	return true;
    }

	bool writeFitting(const winresult_t &res) {
	if (res.type == -1) return false;

	//e.g. save the fitting parameters (for the last window_size/window_slide sticks we can print the relating fitting curve)
	fprintf(fd,"type=%d,Bid,%Ld,%.3f,%.3f,%.3f\n",res.type,res.timestamp,res.p0_bid,res.p1_bid,res.p2_bid);
	fprintf(fd,"type=%d,Ask,%Ld,%.3f,%.3f,%.3f\n",res.type,res.timestamp,res.p0_ask,res.p1_ask,res.p2_ask);
	
	
	return true;
    }

private:
    FILE *fd;
};

#endif /* WRITER_HPP */
