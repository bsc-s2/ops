#!/bin/sh

if [ "$#" = "0" ]; then
    cat <<-END
	usage: $0 <nginx_log_fn> 
	
	Generate an svg web page of distribution of number of request over latency.
	END
    exit 1
fi

fn=${1}

# env:

outputfn=${outputfn-timed.html}
http_port=${http_port-9999}

width=${width-700}
height=${height-300}
padding_left=${padding_left-100}
padding_bottom=${padding_bottom-100}

let image_width=width+padding_left
let image_height=height+padding_bottom
let highest=height*3/4

# 20 px per millisecond
px_per_ms=5

# generate time-no_req table

# tail $fn -n5000 \

cat $fn \
    | awk '{print $12}' \
    | sort -n \
    | uniq -c \
> dots.txt

max_n=$(cat dots.txt | awk '{print $1}' | sort -n |  tail -n1)

{
    # head

    cat <<-END
	<!DOCTYPE HTML>
	
	<html>
	    <head>
	        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
	        <title>Latency Distribution</title>
	    </head>
	    <body>
	        <h1>Latency Distribution</h1>
	        <svg style="top: 100; left: 100; position: absolute" width="$image_width" height="$image_height"
	             xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
	END

    # draw horizontal lines and text for y axis

    for i in $(seq 0 10); do
        let y=height-i*height/10
        let nn=max_n*i/10*height/highest
        let text_x=padding_left-20
        cat <<-END
	        <path d="M$padding_left $y L$image_width $y" stroke="#ddd" stroke-width="1px"></path>
	        <text fill="black" x="$text_x" y="$y" font-size="8pt" font-weight="100" font-family="sans-serif" font-style="normal" text-anchor="middle" dominant-baseline="hanging">
	        $nn</text>
		END
    done

    # draw vertical lines and text for x axis

    let n_y_line=width/50
    for i in $(seq 0 $n_y_line); do

        let x=padding_left+i*50
        let ms=i*50/px_per_ms
        let text_y=height+25
        cat <<-END
	        <path d="M$x 0 L$x $height" stroke="#ddd" stroke-width="1px" ></path>
	
	        <text fill="black" x="$x" y="$text_y" font-size="8pt" font-weight="100" font-family="sans-serif" font-style="normal" text-anchor="middle" dominant-baseline="hanging">
	        ${ms}ms</text>
	
		END
    done

    # generate distribution curve

    cat <<-END
	        <path d="
	END

    cat dots.txt \
        | awk \
            -v step=0.001 \
            -v max_n=$max_n \
            -v height=$height \
            -v padding_left=$padding_left \
            -v highest=$highest \
            -v px_per_ms=$px_per_ms \
    '
    BEGIN {
        tp = "M"
    }

    {

        x = padding_left + $2 * 1000 * px_per_ms
        y = height - $1 * highest / max_n

        print tp x " " y " "

        tp = "L"
    }'


    cat <<-END
	            "
	            stroke="red"
	            fill="none"
	            stroke-width="2.01"
	            stroke-linecap="round"
	            stroke-linejoin="round"
	            clip-path="url(#trace-effect-clip)"
	            >
	            </path>
	END

    # end of file

    cat <<-END
            </svg>

        </body>
    </html>
	END

} \
> $outputfn

echo '== copy one of following url to view =='

for ip in $(ifconfig | grep "inet " | awk '{print $2}' | grep -v 127.0.0.1); do
    echo "http://$ip:$http_port/$outputfn"
done

python -m SimpleHTTPServer $http_port

# vim: ts=4
