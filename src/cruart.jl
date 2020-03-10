module cruart
using Gumbo, CSV, FileWatching, DataFrames, Lazy, TimeZones, Dates

const data_pfx = "data"
const data_fdr = data_pfx * "/"

function run()
    unwatch_folder(data_pfx)
    println("Waiting for data...")
    while true
        new_file = watch_folder(data_pfx)[1]
        if isfile(data_fdr * new_file)
            main(data_fdr * new_file)
        end
    end
end

function parse_cruart_date(str)
    r = r"/"
    m = match(r,str)
    month = parse(Int,str[1:m.offset-1])
    m2 = match(r,str,m.offset+1)
    day = parse(Int,str[m.offset+1:m2.offset-1])
    year = parse(Int,str[length(str)-3:length(str)])
    (year, month, day)
end

function main(filename)
    tz = TimeZone("America/Detroit")
    html_data = try
        parsehtml(read(filename, String))
    catch ex
        println(ex)
        nothing
    end
    if html_data == nothing
        return
    end
    pages = length(html_data.root.children[2].children)

    results_df = DataFrame(trainee = String[], minutes = Int[], training_date = String[], training_pos = String[], ojti = String[])

    for page in 1:pages
        for row in html_data.root.children[2].children[page].children[1].children
            if is_training_row(row)
                trainee = row.children[4].children[1].children[1].text
                trainee = trainee[length(trainee)-1:length(trainee)]
                minutes = parse_minutes(row.children[10].children[1].children[1].text)
                date = row.children[7].children[1].children[1].text
                time = row.children[8].children[1].children[1].text
                dt = ZonedDateTime(parse_cruart_date(date)...,
                                   parse(Int,time[1:2]),
                                   parse(Int,time[4:5]),
                                   TimeZone("UTC"))
                local_dt = Dates.format(astimezone(dt,tz), "mm/dd/yyyy HH:MM")
                pos = row.children[5].children[1].children[1].text
                ojti = row.children[3].children[1].children[1].text
                ojti = ojti[length(ojti)-1:length(ojti)]
                if pos == "RCIC" || pos == "CICA"
                    continue
                else
                    push!(results_df, (trainee, minutes, local_dt, pos, ojti))
                end
            end
        end
    end

    outfile = filename[match(r"\/",filename).offset+1:match(r"\.",filename).offset-1]
    try
        CSV.write(outfile * ".csv",results_df)
        println("Successfully wrote " * outfile * ".csv")
    catch ex
        println("Failed to write " * outfile * ".csv")
        println(ex)
        nothing
    end
end

function parse_minutes(hours_text)
    sep = match(r"\+",hours_text).offset
    hours = parse(Int,hours_text[1:sep-1])
    minutes = parse(Int,hours_text[sep+2:length(hours_text)])
    minutes + hours*60
end

function is_training_row(row)
    length(row.children) == 10 &&
        typeof(row.children[10].children[1].children[1]) == HTMLText &&
        row.children[6].children[1].children[1].text == "T"
end

run()
end # module
