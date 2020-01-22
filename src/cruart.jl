module cruart
using Gumbo, CSV, FileWatching, DataFrames, Lazy

const data_pfx = "data"
const data_fdr = data_pfx * "/"

function run()
    unwatch_folder(data_pfx)
    while true
        new_file = watch_folder(data_pfx)[1]
        if isfile(data_fdr * new_file)
            main(data_fdr * new_file)
        end
    end
end

function main(filename)
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

    results_df = DataFrame(trainee = String[], minutes = Int[], training_date = String[])

    for page in 1:pages
        for row in html_data.root.children[2].children[page].children[1].children
            if is_training_row(row)
                trainee = row.children[4].children[1].children[1].text
                trainee = trainee[length(trainee)-1:length(trainee)]
                minutes = parse_minutes(row.children[10].children[1].children[1].text)
                date = row.children[7].children[1].children[1].text
                push!(results_df, (trainee, minutes, date))
            end
        end
    end

    group_df = @> begin
        results_df
        DataFrames.groupby([:trainee, :training_date])
        aggregate(sum)
        rename((:minutes_sum => :minutes))
    end

    outfile = filename[match(r"\/",filename).offset+1:match(r"\.",filename).offset-1]
    CSV.write(outfile * ".csv",group_df)
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
