#!/usr/bin/env python


sql = {"personal_stats": ("select newid, avg(weight_int) as avg_weight, "
                    "avg(height_inches_int) as avg_height_inches "
                    "from dssg.allegations_master group by newid"),
       "ia_history": ("blah")
       }
